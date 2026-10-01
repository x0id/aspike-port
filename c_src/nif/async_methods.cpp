#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_cdt_order.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_node.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_orderedmap.h>
#include <aerospike/as_pair.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <assert.h>
#include <erl_nif.h>
#include <time.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "aspike_nif.h"
#include "common_methods.h"
#include "async_methods.h"

using namespace std;

// Async callback structure for async operations
struct callback_data {
    // 'pi_env' means Process Independent Environment, and in the async functions below
    // you may notice 'env' variables which refer to Process Dependent Environment.
    // More on both envs here: https://www.erlang.org/doc/apps/erts/erl_nif.html#proc_bound_env
    ErlNifEnv* erl_pi_env = nullptr;
    ErlNifPid caller_pid;
    ERL_NIF_TERM ref;
    vector<as_arraylist> arraylist_vector;
    vector<as_operations> operations_vector;
    as_batch_records batch_records = {};
    bool batch_records_initialized = false;
    string node_name;
    string bin_name;
    vector<string> bin_names;

    callback_data(ErlNifEnv* caller_pd_env, const ERL_NIF_TERM ref_to_save) {
        enif_self(caller_pd_env, &caller_pid);
        // allocates new process independent environment
        erl_pi_env = enif_alloc_env();
        ref = enif_make_copy(erl_pi_env, ref_to_save);
    }

    callback_data(ErlNifEnv* caller_pd_env, const ERL_NIF_TERM ref_to_save,
                  const string& name_space, const string& set_name, const string& record_name)
        : callback_data(caller_pd_env, ref_to_save) {
        if (!statistics_enabled()) return;
        node_name = get_target_node_for_key(name_space.c_str(), set_name.c_str(), record_name.c_str());
        if (node_name.empty()) return;
        auto node_stats = get_or_create_node_stats(node_name);

        // Increment both global and per-node counters
        async_current_counter.fetch_add(1);
        node_stats->async_current.fetch_add(1);

        auto global_value = async_current_counter.load();
        auto node_value = node_stats->async_current.load();
        auto now = unix_ts();

        // Update global peaks
        if (async_peak_counter.load() < global_value || async_peak_ttl_counter.load() < now) {
            async_peak_counter.store(global_value);
            async_peak_ttl_counter.store(now + 15);
        }

        // Update node-specific peaks
        if (node_stats->async_peak.load() < node_value || node_stats->async_peak_ttl.load() < now) {
            node_stats->async_peak.store(node_value);
            node_stats->async_peak_ttl.store(now + 15);
        }
    }

    ~callback_data() {
        if (!node_name.empty() && statistics_enabled()) {
            async_current_counter.fetch_sub(1);
            auto node_stats = get_or_create_node_stats(node_name);
            node_stats->async_current.fetch_sub(1);
        }
        if (batch_records_initialized) {
            as_batch_records_destroy(&batch_records);
        }
        for (auto& arraylist : arraylist_vector) {
            as_arraylist_destroy(&arraylist);
        }
        for (auto& operations : operations_vector) {
            as_operations_destroy(&operations);
        }
        if (erl_pi_env) {
            enif_free_env(erl_pi_env);
            erl_pi_env = nullptr;
        }
    }
};

static void cdt_put_async_callback(as_error* err, as_record* record, void* udata, as_event_loop* event_loop) {
    callback_data* cb_data = (callback_data*)udata;

    ERL_NIF_TERM result_msg;

    if (err) {
        ERL_NIF_TERM error_msg;

        if (err->code == AEROSPIKE_ERR_NO_MORE_CONNECTIONS) {
            // Special handling for connection pool exhaustion
            error_msg = enif_make_string(cb_data->erl_pi_env, "connection_pool_exhausted", ERL_NIF_UTF8);
        } else {
            // Regular error message
            if (strlen(err->message) != 0) {
                error_msg = enif_make_string(cb_data->erl_pi_env, err->message, ERL_NIF_UTF8);
            } else {
                error_msg = enif_make_string(cb_data->erl_pi_env, "Unknown error occurred", ERL_NIF_UTF8);
            }
        }
        auto nifErrorCode = enif_make_int(cb_data->erl_pi_env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(cb_data->erl_pi_env, err->code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(cb_data->erl_pi_env, nifErrorCode, aspikeErrorCode, error_msg);

        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_error, cb_data->ref, error_tuple);
    } else {
        ERL_NIF_TERM response = enif_make_string(cb_data->erl_pi_env, "put", ERL_NIF_UTF8);
        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_ok, cb_data->ref, response);
    }

    enif_send(NULL, &cb_data->caller_pid, cb_data->erl_pi_env, result_msg);
    delete cb_data;
}

ERL_NIF_TERM aspike_nif_cdt_put_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_namespace, erl_set_name, erl_record_name;
    if (!enif_is_ref(env, argv[0])) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_namespace)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_record_name)) {
        return enif_make_badarg(env);
    }

    string name_space = string((const char*)erl_namespace.data, erl_namespace.size);
    string set_name = string((const char*)erl_set_name.data, erl_set_name.size);
    string record_name((const char*)erl_record_name.data, erl_record_name.size);

    unsigned int bins_amount;
    ERL_NIF_TERM bins = argv[4];
    if (!enif_is_list(env, bins) || !enif_get_list_length(env, bins, &bins_amount)) {
        return enif_make_badarg(env);
    }
    // now the list points to structure like
    // [{<<"fcap_map">>, [<<"map_1_name">>, <<"map_1_value">>, 123, <<"map_2_name">>, <<"map_2_value">>, 456]}]
    if (bins_amount == 0) {
        // just a check if there is something we should do at all, and if not - complete this call
        auto msg = enif_make_string(env, "put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, atom_ok, msg);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    long ttl;
    if (!enif_get_long(env, argv[5], &ttl)) {
        return enif_make_badarg(env);
    }

    const ERL_NIF_TERM* erl_policy = NULL;
    int policy_length;
    long max_retries = 0;
    long socket_timeout = 0;
    long total_timeout = 0;
    int policyReadRC = enif_get_tuple(env, argv[6], &policy_length, &erl_policy);
    if (!policyReadRC || policy_length != 4) {
        return enif_make_badarg(env);
    }
    // Note: sleep_between_retries is not used in async operations by c-client
    enif_get_long(env, erl_policy[0], &max_retries);
    enif_get_long(env, erl_policy[2], &socket_timeout);
    enif_get_long(env, erl_policy[3], &total_timeout);
    as_policy_operate policy;
    as_policy_operate_init(&policy);
    policy.ttl = ttl;
    policy.base.max_retries = max_retries;
    policy.base.socket_timeout = socket_timeout;
    policy.base.total_timeout = total_timeout;

    // We need to know upfront how many operations we are going to send to
    // aerospike, and we can get that amount by walking thr provided bins
    // and checking the lengths of data of bins.
    // At the same time this is a good place to conduct some input validation
    // so we don't have to do that later once we start memory allocations
    uint total_operations = 0;
    ERL_NIF_TERM bins_copy = bins; // Keep original for second pass
    for (uint i = 0; i < bins_amount; i++) {
        ERL_NIF_TERM bins_head, bins_tail;
        if (!enif_get_list_cell(env, bins_copy, &bins_head, &bins_tail)) {
            return enif_make_badarg(env);
        }

        int tuple_length;
        const ERL_NIF_TERM* bin_tuple = NULL;
        if (!enif_get_tuple(env, bins_head, &tuple_length, &bin_tuple) || tuple_length != 2) {
            return enif_make_badarg(env);
        }

        ErlNifBinary erl_bin_name;
        if (!enif_inspect_binary(env, bin_tuple[0], &erl_bin_name)) {
            return enif_make_badarg(env);
        }

        unsigned int bin_data_len;
        if (!enif_is_list(env, bin_tuple[1]) || !enif_get_list_length(env, bin_tuple[1], &bin_data_len)) {
            return enif_make_badarg(env);
        }
        total_operations += bin_data_len;
        bins_copy = bins_tail;
    }
    as_operations operations;
    as_operations_inita(&operations, total_operations);
    if (ttl != 0) {
        operations.ttl = ttl;
    } else {
        operations.ttl = -2;
    }

    as_key record_key;
    as_key_init_str(&record_key, name_space.c_str(), set_name.c_str(), record_name.c_str());

    vector<as_cdt_ctx*> cdt_contexts;

    auto cb_data = new callback_data(env, argv[0], name_space, set_name, record_name);

    as_map_policy put_mode;
    as_map_policy_set(&put_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);

    for (uint i = 0; i < bins_amount; i++) {
        // each bin of
        // {<<"fcap_map">>, [<<"map_1_name">>, <<"map_1_value">>, 123, <<"map_2_name">>, <<"map_2_value">>, 456]}
        // will form a next map:
        // KEY_ORDERED_MAP('{"map_1_name":{"ttl":123, "value":"map_1_value", "wt":1766794574}, "map_2_name":{"ttl":456, "value":"map_2_value", "wt":1766794574}}')
        // which will be stored under a bin name "fcap_map".

        ERL_NIF_TERM bins_head, bins_tail;
        enif_get_list_cell(env, bins, &bins_head, &bins_tail);
        // now, the bins_head points to something like {<<"fcap_map">>, [<<"map_1_name">>, <<"map_1_value">>, 123, <<"map_2_name">>, <<"map_2_value">>, 456]}

        int tuple_length;
        const ERL_NIF_TERM* bin_tuple = NULL;
        enif_get_tuple(env, bins_head, &tuple_length, &bin_tuple);
        // now bin_tuple points to something like {<<"fcap_map">>, [<<"map_1_name">>, <<"map_1_value">>, 123, <<"map_2_name">>, <<"map_2_value">>, 456]}

        ErlNifBinary erl_bin_name;
        enif_inspect_binary(env, bin_tuple[0], &erl_bin_name);
        string bin_name;
        bin_name.assign((const char*)erl_bin_name.data, erl_bin_name.size);
        // now bin_name has a value like "fcap_map"

        unsigned int bin_data_len;
        enif_get_list_length(env, bin_tuple[1], &bin_data_len);
        auto bin_data = bin_tuple[1];
        // bin_data points to something like [<<"map_1_name">>, <<"map_1_value">>, 123, <<"map_2_name">>, <<"map_2_value">>, 456]

        uint opnum = 0;
        for (uint k = 0; k < bin_data_len; k++) {
            ERL_NIF_TERM data_head;
            ERL_NIF_TERM data_tail;
            if (!enif_get_list_cell(env, bin_data, &data_head, &data_tail)) {
                break;
            }

            // the following code forms a map with structure like:
            // KEY_ORDERED_MAP(
            //     "map_1_name": {
            //         "ttl":123,
            //         "value":"map_1_value",
            //         "wt":1766794574
            //     },
            //     "map_2_name": {
            //         "ttl":456,
            //         "value":"map_2_value",
            //         "wt":1766794574
            //     }
            //     ...
            // )
            // the first-level map key name and the values of second-level 'value' and 'ttl'
            // keys are read one by one from 'bin_data' array with 'opnum' variable indicating
            // which value we are reading now

            if (opnum == 0) {
                // with optnum == 0 it's a first-level key name.
                // The value of this key will be another map, so let's create a context for
                // this map
                as_cdt_ctx* context = as_cdt_ctx_create(1);
                // save context for later removal
                cdt_contexts.push_back(context);
                // getting first level key name
                ErlNifBinary erl_key_name;
                if (enif_inspect_binary(env, data_head, &erl_key_name)) {
                    // erl_key_name points to something like <<"map_1_name">>.
                    // Now, create an aerospike string which will be freed by context on its removal,
                    // and use erlang term (the data of which wil be available during this function lifespan) to
                    // feed the string data. The 'false' flag means no memory free for that data.
                    // Now, make a copy of the string on heap (because the erl_key_name located on stack)
                    // so Aerospike will be able to free its memory once the 'key_name' variable will be destroyed.
                    // Allocate size + 1 for null terminator
                    uint8_t* copy_on_heap = (uint8_t*)malloc(sizeof(uint8_t) * (erl_key_name.size + 1));
                    memcpy(copy_on_heap, erl_key_name.data, erl_key_name.size);
                    copy_on_heap[erl_key_name.size] = '\0';  // Null terminate the string
                    // create aerospike string which will be freed by context on its removal
                    as_string* key_name = as_string_new((char*)copy_on_heap, true);
                    as_cdt_ctx_add_map_key_create(context, (as_val*)key_name, AS_MAP_KEY_ORDERED);
                }
                opnum++;
            } else if (opnum == 1) {
                // with optnum == 1 it's a value for 'value' key on second-level map
                ErlNifBinary erl_value_data;
                if (enif_inspect_binary(env, data_head, &erl_value_data)) {
                    // erl_value_data points to something like <<"map_1_value">>.
                    // Create aerospike string (a second level key name) which will be freed by context on its removal.
                    as_string* key_name = as_string_new_strdup("value");

                    // Use the data from erlang term (the data of which wil be available during this function lifespan).
                    // The 'false' flag means no memory free for that data.
                    as_bytes* value_data = as_bytes_new_wrap(erl_value_data.data, erl_value_data.size, false);
                    // next line creates a key 'value' in the map we created above in 'opnum == 1'
                    as_operations_map_put(&operations, bin_name.c_str(), cdt_contexts.back(), &put_mode, (as_val*)key_name, (as_val*)value_data);
                }
                opnum++;
            } else if (opnum == 2) {
                // with optnum == 2 it's a value for 'ttl' key on second-level map
                long i64;
                if (enif_get_int64(env, data_head, &i64)) {
                    // i64 points to something like 123.
                    // Create aerospike string (a second level key name) which will be freed by context on its removal.
                    as_string* key_name = as_string_new_strdup("ttl");
                    as_integer* ttl_value = as_integer_new(i64);
                    // next line creates a key 'ttl' in the map we created above in 'opnum == 1'
                    as_operations_map_put(&operations, bin_name.c_str(), cdt_contexts.back(), &put_mode, (as_val*)key_name, (as_val*)ttl_value);
                }

                // let's create a 'wt' key on second-level map
                // Create aerospike string (a second level key name) which will be freed by context on its removal.
                as_string* key_name = as_string_new_strdup("wt");
                auto now = chrono::system_clock::now().time_since_epoch();
                long timestamp = chrono::duration_cast<chrono::seconds>(now).count();
                as_integer* wt_value = as_integer_new(timestamp);
                // next line creates a key 'wt' in the map we created above in 'opnum == 1'
                as_operations_map_put(&operations, bin_name.c_str(), cdt_contexts.back(), &put_mode, (as_val*)key_name, (as_val*)wt_value);

                opnum = 0;
            } else {
                break;
            }

            bin_data = data_tail;
        }
        bins = bins_tail;
    }

    as_error err;
    as_status status = aerospike_key_operate_async(as, &err, &policy, &record_key, &operations, cdt_put_async_callback, cb_data, NULL, NULL);

    as_operations_destroy(&operations);
    for (auto cdt_ctx : cdt_contexts) {
        as_cdt_ctx_destroy(cdt_ctx);
    }
    as_key_destroy(&record_key);

    if (status != AEROSPIKE_OK) {
        delete cb_data;

        ERL_NIF_TERM error_msg;
        if (strlen(err.message) != 0) {
            error_msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        } else {
            error_msg = enif_make_string(env, "Unknown error occurred", ERL_NIF_UTF8);
        }
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, error_msg);

        return_data = enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        return_data = enif_make_tuple2(env, atom_ok, atom_in_progress);
    }

    return return_data;
}

static void cdt_get_async_callback(as_error* err, as_record* record, void* udata, as_event_loop* event_loop) {
    callback_data* cb_data = (callback_data*)udata;

    ERL_NIF_TERM result_msg;

    if (err) {
        ERL_NIF_TERM error_msg;

        if (err->code == AEROSPIKE_ERR_NO_MORE_CONNECTIONS) {
            // Special handling for connection pool exhaustion
            error_msg = enif_make_string(cb_data->erl_pi_env, "connection_pool_exhausted", ERL_NIF_UTF8);
        } else {
            // Regular error message
            if (strlen(err->message) != 0) {
                error_msg = enif_make_string(cb_data->erl_pi_env, err->message, ERL_NIF_UTF8);
            } else {
                error_msg = enif_make_string(cb_data->erl_pi_env, "Unknown error occurred", ERL_NIF_UTF8);
            }
        }
        auto nifErrorCode = enif_make_int(cb_data->erl_pi_env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(cb_data->erl_pi_env, err->code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(cb_data->erl_pi_env, nifErrorCode, aspikeErrorCode, error_msg);

        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_error, cb_data->ref, error_tuple);
    } else {
        ERL_NIF_TERM response = aspike_dump_cdt_records(cb_data->erl_pi_env, record);
        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_ok, cb_data->ref, response);
    }

    enif_send(NULL, &cb_data->caller_pid, cb_data->erl_pi_env, result_msg);
    delete cb_data;
}

ERL_NIF_TERM aspike_nif_cdt_get_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_namespace, erl_set_name, erl_record_name;
    if (!enif_is_ref(env, argv[0])) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_namespace)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_record_name)) {
        return enif_make_badarg(env);
    }

    string set_name((const char*)erl_set_name.data, erl_set_name.size);
    string name_space((const char*)erl_namespace.data, erl_namespace.size);
    string record_name((const char*)erl_record_name.data, erl_record_name.size);

    const ERL_NIF_TERM* erl_policy = NULL;
    int policy_length;
    long max_retries = 0;
    long socket_timeout = 0;
    long total_timeout = 0;
    int policyReadRC = enif_get_tuple(env, argv[4], &policy_length, &erl_policy);
    if (!policyReadRC || policy_length != 4) {
        return enif_make_badarg(env);
    }
    // note: sleep_between_retries is not used in async operations by c-client
    enif_get_long(env, erl_policy[0], &max_retries);
    enif_get_long(env, erl_policy[2], &socket_timeout);
    enif_get_long(env, erl_policy[3], &total_timeout);
    as_policy_read policy;
    as_policy_read_init(&policy);
    policy.base.max_retries = max_retries;
    policy.base.socket_timeout = socket_timeout;
    policy.base.total_timeout = total_timeout;

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    as_key record_key;
    as_key_init_str(&record_key, name_space.c_str(), set_name.c_str(), record_name.c_str());

    auto cb_data = new callback_data(env, argv[0], name_space, set_name, record_name);

    as_error err;
    as_status status = aerospike_key_get_async(as, &err, &policy, &record_key, cdt_get_async_callback, cb_data, NULL, NULL);

    as_key_destroy(&record_key);

    if (status != AEROSPIKE_OK) {
        delete cb_data;

        ERL_NIF_TERM error_msg;
        if (strlen(err.message) != 0) {
            error_msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        } else {
            error_msg = enif_make_string(env, "Unknown error occurred", ERL_NIF_UTF8);
        }
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, error_msg);

        return_data = enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        return_data = enif_make_tuple2(env, atom_ok, atom_in_progress);
    }

    return return_data;
}

ERL_NIF_TERM aspike_nif_cdt_select_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_namespace, erl_set_name, erl_record_name;
    if (!enif_is_ref(env, argv[0])) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_namespace)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_record_name)) {
        return enif_make_badarg(env);
    }

    // argv[4] is a list of bin name binaries
    unsigned int bin_list_len;
    if (!enif_get_list_length(env, argv[4], &bin_list_len) || bin_list_len == 0) {
        return enif_make_badarg(env);
    }

    string set_name((const char*)erl_set_name.data, erl_set_name.size);
    string name_space((const char*)erl_namespace.data, erl_namespace.size);
    string record_name((const char*)erl_record_name.data, erl_record_name.size);

    // argv[5] is policy tuple
    const ERL_NIF_TERM* erl_policy = NULL;
    int policy_length;
    long max_retries = 0;
    long socket_timeout = 0;
    long total_timeout = 0;
    int policyReadRC = enif_get_tuple(env, argv[5], &policy_length, &erl_policy);
    if (!policyReadRC || policy_length != 4) {
        return enif_make_badarg(env);
    }
    enif_get_long(env, erl_policy[0], &max_retries);
    enif_get_long(env, erl_policy[2], &socket_timeout);
    enif_get_long(env, erl_policy[3], &total_timeout);
    as_policy_read policy;
    as_policy_read_init(&policy);
    policy.base.max_retries = max_retries;
    policy.base.socket_timeout = socket_timeout;
    policy.base.total_timeout = total_timeout;

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    as_key record_key;
    as_key_init_str(&record_key, name_space.c_str(), set_name.c_str(), record_name.c_str());

    auto cb_data = new callback_data(env, argv[0], name_space, set_name, record_name);

    // Parse bin names list and store in cb_data->bin_names
    ERL_NIF_TERM list = argv[4];
    ERL_NIF_TERM head;
    cb_data->bin_names.reserve(bin_list_len);
    for (unsigned int i = 0; i < bin_list_len; i++) {
        enif_get_list_cell(env, list, &head, &list);
        ErlNifBinary bin;
        if (!enif_inspect_binary(env, head, &bin)) {
            delete cb_data;
            as_key_destroy(&record_key);
            return enif_make_badarg(env);
        }
        cb_data->bin_names.emplace_back((const char*)bin.data, bin.size);
    }

    // Build NULL-terminated array of C strings pointing into cb_data->bin_names
    vector<const char*> bins_to_read(bin_list_len + 1);
    for (unsigned int i = 0; i < bin_list_len; i++) {
        bins_to_read[i] = cb_data->bin_names[i].c_str();
    }
    bins_to_read[bin_list_len] = NULL;

    as_error err;
    as_status status = aerospike_key_select_async(as, &err, &policy, &record_key, bins_to_read.data(), cdt_get_async_callback, cb_data, NULL, NULL);

    as_key_destroy(&record_key);

    if (status != AEROSPIKE_OK) {
        delete cb_data;

        ERL_NIF_TERM error_msg;
        if (strlen(err.message) != 0) {
            error_msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        } else {
            error_msg = enif_make_string(env, "Unknown error occurred", ERL_NIF_UTF8);
        }
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, error_msg);

        return_data = enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        return_data = enif_make_tuple2(env, atom_ok, atom_in_progress);
    }

    return return_data;
}

static void cdt_delete_by_keys_async_callback(as_error* err, as_record* record, void* udata, as_event_loop* event_loop) {
    callback_data* cb_data = (callback_data*)udata;

    ERL_NIF_TERM result_msg;

    if (err) {
        ERL_NIF_TERM error_msg;

        if (err->code == AEROSPIKE_ERR_NO_MORE_CONNECTIONS) {
            // Special handling for connection pool exhaustion
            error_msg = enif_make_string(cb_data->erl_pi_env, "connection_pool_exhausted", ERL_NIF_UTF8);
        } else {
            // Regular error message
            if (strlen(err->message) != 0) {
                error_msg = enif_make_string(cb_data->erl_pi_env, err->message, ERL_NIF_UTF8);
            } else {
                error_msg = enif_make_string(cb_data->erl_pi_env, "Unknown error occurred", ERL_NIF_UTF8);
            }
        }
        auto nifErrorCode = enif_make_int(cb_data->erl_pi_env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(cb_data->erl_pi_env, err->code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(cb_data->erl_pi_env, nifErrorCode, aspikeErrorCode, error_msg);

        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_error, cb_data->ref, error_tuple);
    } else {
        ERL_NIF_TERM response = enif_make_string(cb_data->erl_pi_env, "keys_deleted", ERL_NIF_UTF8);
        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_ok, cb_data->ref, response);
    }

    enif_send(NULL, &cb_data->caller_pid, cb_data->erl_pi_env, result_msg);
    delete cb_data;
}

ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_ns, erl_set_name, erl_record_name, erl_bin_name;
    if (!enif_is_ref(env, argv[0])) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_ns)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_record_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[4], &erl_bin_name)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM list = argv[5];
    unsigned int length;
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    string name_space((const char*)erl_ns.data, erl_ns.size);
    string set_name((const char*)erl_set_name.data, erl_set_name.size);
    string record_name((const char*)erl_record_name.data, erl_record_name.size);
    string bin_name((const char*)erl_bin_name.data, erl_bin_name.size);

    as_arraylist remove_list;
    as_arraylist_init(&remove_list, length, length);

    as_key record_key;
    as_key_init_str(&record_key, name_space.c_str(), set_name.c_str(), record_name.c_str());

    as_operations operations;
    as_operations_inita(&operations, 1);
    operations.ttl = AS_RECORD_NO_CHANGE_TTL;  // Preserve existing record TTL (-2)
    as_map_policy put_mode;
    as_map_policy_set(&put_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);

    ErlNifBinary subkey_term;
    string subkey_str;
    unsigned int subkeys_num = 0;
    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (enif_inspect_binary(env, head, &subkey_term)) {
            // we have to allocate memory for subkey_str because function subkey_term.data
            // may be not a null-terminated string, and passing subkey_term.data to
            // as_arraylist_append_str() might be a bad idea.
            subkey_str.assign((const char*)subkey_term.data, subkey_term.size);
            as_arraylist_append_str(&remove_list, (char*)subkey_str.c_str());
            subkeys_num++;
        }
        list = tail;
    }
    // we need to be sure we put all keys into the list, otherwise the behavior
    // of operation can be undefined as we defined the length of keys to remove
    if (subkeys_num == length) {
        as_operations_add_map_remove_by_key_list(&operations, bin_name.c_str(), (as_list*)&remove_list, AS_MAP_RETURN_NONE);
    }

    auto cb_data = new callback_data(env, argv[0], name_space, set_name, record_name);

    as_error err;
    as_status status = aerospike_key_operate_async(as, &err, NULL, &record_key, &operations, cdt_delete_by_keys_async_callback, cb_data, NULL, NULL);

    as_operations_destroy(&operations);
    as_arraylist_destroy(&remove_list);
    as_key_destroy(&record_key);

    if (status != AEROSPIKE_OK) {
        delete cb_data;

        ERL_NIF_TERM error_msg;
        if (strlen(err.message) != 0) {
            error_msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        } else {
            error_msg = enif_make_string(env, "Unknown error occurred", ERL_NIF_UTF8);
        }
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, error_msg);

        return_data = enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        return_data = enif_make_tuple2(env, atom_ok, atom_in_progress);
    }

    return return_data;
}

void cdt_delete_by_keys_batch_async_callback(as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop) {
    callback_data* cb_data = (callback_data*)udata;

    ERL_NIF_TERM result_msg;

    if (err && err->code != AEROSPIKE_OK) {
        ERL_NIF_TERM error_msg;

        if (err->code == AEROSPIKE_ERR_NO_MORE_CONNECTIONS) {
            error_msg = enif_make_string(cb_data->erl_pi_env, "connection_pool_exhausted", ERL_NIF_UTF8);
        } else {
            if (strlen(err->message) != 0) {
                error_msg = enif_make_string(cb_data->erl_pi_env, err->message, ERL_NIF_UTF8);
            } else {
                error_msg = enif_make_string(cb_data->erl_pi_env, "Unknown error occurred", ERL_NIF_UTF8);
            }
        }
        auto nifErrorCode = enif_make_int(cb_data->erl_pi_env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(cb_data->erl_pi_env, err->code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(cb_data->erl_pi_env, nifErrorCode, aspikeErrorCode, error_msg);

        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_error, cb_data->ref, error_tuple);
    } else {
        vector<ERL_NIF_TERM> erl_list;
        for (uint32_t i = 0; i < records->list.size; i++) {
            as_batch_write_record* record = (as_batch_write_record*)as_vector_get(&records->list, i);
            erl_list.push_back(enif_make_int(cb_data->erl_pi_env, record->result));
        }
        auto result_list = enif_make_list_from_array(cb_data->erl_pi_env, erl_list.data(), erl_list.size());
        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_ok, cb_data->ref, result_list);
    }

    enif_send(NULL, &cb_data->caller_pid, cb_data->erl_pi_env, result_msg);
    delete cb_data;
}

ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_batch_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_ns, erl_set_name, erl_bin_name;
    if (!enif_is_ref(env, argv[0])) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_ns)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_bin_name)) {
        return enif_make_badarg(env);
    }
    ERL_NIF_TERM erl_tuples = argv[4];
    unsigned int tuples_amount;
    if (!enif_is_list(env, erl_tuples) || !enif_get_list_length(env, erl_tuples, &tuples_amount)) {
        return enif_make_badarg(env);
    }
    // erl_tuples is a list of next tuples:
    // [{PrimaryKey, [MapKey1, MapKey2, ...]}, {...}, ...]
    // where in each tuple the PrimaryKey points to the record in which we are going to
    // remove keys MapKey1, MapKey2, etc. from the CDT map located in the bin named erl_bin_name

    ERL_NIF_TERM erl_current_list = erl_tuples;
    ERL_NIF_TERM erl_head;
    ERL_NIF_TERM erl_tail;
    const ERL_NIF_TERM* erl_tuple = nullptr;
    ErlNifBinary erl_pk_name;
    unsigned int tuple_index;
    int tuple_size;
    unsigned int map_keys_amount;
    ERL_NIF_TERM erl_map_keys;
    ERL_NIF_TERM erl_map_keys_head;
    ERL_NIF_TERM erl_map_keys_tail;
    ErlNifBinary erl_map_key_name;
    for (tuple_index = 0; tuple_index < tuples_amount; tuple_index++) {
        if (!enif_get_list_cell(env, erl_current_list, &erl_head, &erl_tail)) {
            return enif_make_badarg(env);
        }
        if (!enif_get_tuple(env, erl_head, &tuple_size, &erl_tuple) || tuple_size != 2) {
            return enif_make_badarg(env);
        }
        if (!enif_inspect_binary(env, erl_tuple[0], &erl_pk_name)) {
            return enif_make_badarg(env);
        }
        if (!enif_is_list(env, erl_tuple[1]) || !enif_get_list_length(env, erl_tuple[1], &map_keys_amount)) {
            return enif_make_badarg(env);
        }
        erl_map_keys = erl_tuple[1];
        for (uint k = 0; k < map_keys_amount; k++) {
            if (!enif_get_list_cell(env, erl_map_keys, &erl_map_keys_head, &erl_map_keys_tail)) {
                return enif_make_badarg(env);
            }
            if (!enif_inspect_binary(env, erl_map_keys_head, &erl_map_key_name)) {
                return enif_make_badarg(env);
            }
            erl_map_keys = erl_map_keys_tail;
        }
        erl_current_list = erl_tail;
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    string name_space((const char*)erl_ns.data, erl_ns.size);
    string set_name((const char*)erl_set_name.data, erl_set_name.size);
    string bin_name((const char*)erl_bin_name.data, erl_bin_name.size);

    vector<string> pk_names(tuples_amount);
    vector<as_batch_write_record*> batch_writes(tuples_amount);
    string map_key_name;

    auto cb_data = new callback_data(env, argv[0]);
    cb_data->operations_vector.resize(tuples_amount);
    cb_data->arraylist_vector.resize(tuples_amount);
    as_batch_records_init(&cb_data->batch_records, tuples_amount);
    cb_data->batch_records_initialized = true;

    erl_current_list = erl_tuples;
    for (uint tuple_index = 0; tuple_index < tuples_amount; tuple_index++) {
        enif_get_list_cell(env, erl_current_list, &erl_head, &erl_tail);
        enif_get_tuple(env, erl_head, &tuple_size, &erl_tuple);

        enif_inspect_binary(env, erl_tuple[0], &erl_pk_name);
        pk_names[tuple_index].assign((const char*)erl_pk_name.data, erl_pk_name.size);

        enif_is_list(env, erl_tuple[1]);
        enif_get_list_length(env, erl_tuple[1], &map_keys_amount);

        batch_writes[tuple_index] = as_batch_write_reserve(&cb_data->batch_records);
        as_key_init_str(&(batch_writes[tuple_index]->key), name_space.c_str(), set_name.c_str(), pk_names[tuple_index].c_str());

        as_arraylist_init(&(cb_data->arraylist_vector[tuple_index]), map_keys_amount, map_keys_amount);

        erl_map_keys = erl_tuple[1];
        for (uint k = 0; k < map_keys_amount; k++) {
            enif_get_list_cell(env, erl_map_keys, &erl_map_keys_head, &erl_map_keys_tail);
            enif_inspect_binary(env, erl_map_keys_head, &erl_map_key_name);

            map_key_name.assign((const char*)erl_map_key_name.data, erl_map_key_name.size);
            // we can use stack variable here because as_arraylist_append_str will strdup(map_key_name)
            // and free the string automatically once the map_keys[tuple_index] is destroyed
            as_arraylist_append_str(&(cb_data->arraylist_vector[tuple_index]), (char*)map_key_name.c_str());

            erl_map_keys = erl_map_keys_tail;
        }

        as_operations_init(&(cb_data->operations_vector[tuple_index]), 1);
        as_operations_add_map_remove_by_key_list(&(cb_data->operations_vector[tuple_index]), bin_name.c_str(), (as_list*)&(cb_data->arraylist_vector[tuple_index]), AS_MAP_RETURN_NONE);
        cb_data->operations_vector[tuple_index].ttl = AS_RECORD_NO_CHANGE_TTL;
        batch_writes[tuple_index]->ops = &(cb_data->operations_vector[tuple_index]);

        erl_current_list = erl_tail;
    }

    as_error err;
    as_status status = aerospike_batch_write_async(as, &err, nullptr, &cb_data->batch_records, cdt_delete_by_keys_batch_async_callback, cb_data, nullptr);

    if (status != AEROSPIKE_OK) {
        delete cb_data;

        ERL_NIF_TERM error_msg;
        if (strlen(err.message) != 0) {
            error_msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        } else {
            error_msg = enif_make_string(env, "Unknown error occurred", ERL_NIF_UTF8);
        }
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, error_msg);

        return_data = enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        // we don't do tracking of async node connections here because this operations will be executed
        // over several different nodes, and taking into consideration the complexity of implementing
        // support of this (we need to store all node names for all PKs) + quite little percentage of
        // cdt_delete_by_keys_batch() calls compare to cdt_put() and cdt_get(), there is no sense to
        // do this tracking.

        return_data = enif_make_tuple2(env, atom_ok, atom_in_progress);
    }

    return return_data;
}

void segment_tag_get_async_callback(as_error* err, as_record* records, void* udata, as_event_loop* event_loop) {
    callback_data* cb_data = (callback_data*)udata;

    ERL_NIF_TERM result_msg;

    if (err) {
        ERL_NIF_TERM error_msg;

        if (err->code == AEROSPIKE_ERR_NO_MORE_CONNECTIONS) {
            // Special handling for connection pool exhaustion
            error_msg = enif_make_string(cb_data->erl_pi_env, "connection_pool_exhausted", ERL_NIF_UTF8);
        } else {
            // Regular error message
            if (strlen(err->message) != 0) {
                error_msg = enif_make_string(cb_data->erl_pi_env, err->message, ERL_NIF_UTF8);
            } else {
                error_msg = enif_make_string(cb_data->erl_pi_env, "Unknown error occurred", ERL_NIF_UTF8);
            }
        }
        auto nifErrorCode = enif_make_int(cb_data->erl_pi_env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(cb_data->erl_pi_env, err->code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(cb_data->erl_pi_env, nifErrorCode, aspikeErrorCode, error_msg);

        result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_error, cb_data->ref, error_tuple);
    } else {
        try {
            if (records == NULL) throw "NULL p_rec";
            as_bin_value* bin = as_record_get(records, cb_data->bin_name.c_str());
            if (bin == NULL) {
                throw "NULL val - internal error";
            }

            auto bin_type = as_val_type(bin);

            if (bin_type != AS_STRING && bin_type != AS_MAP) {
                throw "Non-string or non-map bin - internal error";
            }

            ERL_NIF_TERM response;
            if (bin_type == AS_STRING) {
                uint8_t* bin_as_str = (uint8_t*)bin->string.value;
                auto len = bin->string.len;

                unsigned char* val_data;
                val_data = enif_make_new_binary(cb_data->erl_pi_env, len, &response);
                memcpy(val_data, bin_as_str, len);
            } else if (bin_type == AS_MAP) {
                as_map* map = (as_map*)bin;
                uint32_t map_size = as_map_size(map);

                ERL_NIF_TERM keys[map_size];
                ERL_NIF_TERM values[map_size];

                as_orderedmap_iterator it;
                as_orderedmap_iterator_init(&it, (as_orderedmap*)map);

                uint32_t idx = 0;
                while (as_orderedmap_iterator_has_next(&it)) {
                    auto item = as_orderedmap_iterator_next(&it);
                    as_pair* pair = as_pair_fromval(item);
                    as_val* key_name = as_pair_1(pair);

                    keys[idx] = aspike_get_binary_from_asval(cb_data->erl_pi_env, key_name);

                    as_val* value = as_pair_2(pair);
                    values[idx] = aspike_get_binary_from_asval(cb_data->erl_pi_env, value);

                    idx++;
                }
                as_orderedmap_iterator_destroy(&it);

                enif_make_map_from_arrays(cb_data->erl_pi_env, keys, values, map_size, &response);
            }

            result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_ok, cb_data->ref, response);
        } catch (char* reason) {
            auto nifErrorCode = enif_make_int(cb_data->erl_pi_env, ASPIKE_NIF_OK);
            auto aspikeErrorCode = enif_make_int(cb_data->erl_pi_env, int(AEROSPIKE_ERR));
            ERL_NIF_TERM error_msg = enif_make_string(cb_data->erl_pi_env, reason, ERL_NIF_UTF8);
            ERL_NIF_TERM error_tuple = enif_make_tuple3(cb_data->erl_pi_env, nifErrorCode, aspikeErrorCode, error_msg);
            result_msg = enif_make_tuple3(cb_data->erl_pi_env, atom_error, cb_data->ref, error_tuple);
        }
    }

    enif_send(NULL, &cb_data->caller_pid, cb_data->erl_pi_env, result_msg);
    delete cb_data;
}

ERL_NIF_TERM aspike_nif_segment_tag_get_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_ns, erl_set_name, erl_record_name, erl_bin_name;
    if (!enif_is_ref(env, argv[0])) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_ns)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_record_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[4], &erl_bin_name)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    string name_space((const char*)erl_ns.data, erl_ns.size);
    string set_name((const char*)erl_set_name.data, erl_set_name.size);
    string record_name((const char*)erl_record_name.data, erl_record_name.size);
    as_key record_key;
    as_key_init_str(&record_key, name_space.c_str(), set_name.c_str(), record_name.c_str());

    auto cb_data = new callback_data(env, argv[0], name_space, set_name, record_name);
    // we save bin_name because we'll need it in the segment_tag_get_async_callback()
    cb_data->bin_name.assign((const char*)erl_bin_name.data, erl_bin_name.size);

    const char* bins_to_read[] = {cb_data->bin_name.c_str(), NULL};

    as_error err;
    as_status status = aerospike_key_select_async(as, &err, nullptr, &record_key, bins_to_read, segment_tag_get_async_callback, cb_data, nullptr, nullptr);

    as_key_destroy(&record_key);

    if (status != AEROSPIKE_OK) {
        delete cb_data;

        ERL_NIF_TERM error_msg;
        if (strlen(err.message) != 0) {
            error_msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        } else {
            error_msg = enif_make_string(env, "Unknown error occurred", ERL_NIF_UTF8);
        }
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, error_msg);

        return_data = enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        return_data = enif_make_tuple2(env, atom_ok, atom_in_progress);
    }

    return return_data;
}
