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
#include <memory>

#include "aspike_nif.h"
#include "common_methods.h"
#include "sync_methods.h"

using namespace std;

// RAII helper for automatically managing sync operation counters
class SyncOperationCounter {
public:
    explicit SyncOperationCounter() {
        if (!statistics_enabled()) return;

        sync_current_counter.fetch_add(1);

        auto value = sync_current_counter.load();
        auto now = unix_ts();
        if (value > sync_peak_counter.load() || now > sync_peak_ttl_counter.load()) {
            sync_peak_counter.store(value);
            sync_peak_ttl_counter.store(now + 15);
        }
    }

    ~SyncOperationCounter() {
        if (!statistics_enabled()) return;
        sync_current_counter.fetch_sub(1);
    }

    // Disable copy constructor and assignment operator
    SyncOperationCounter(const SyncOperationCounter&) = delete;
    SyncOperationCounter& operator=(const SyncOperationCounter&) = delete;
};

ERL_NIF_TERM aspike_nif_cdt_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    unsigned int length;
    string name_space, aspk_set, aspk_key;
    long ttl;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    if (!enif_get_long(env, argv[4], &ttl)) {
        return enif_make_badarg(env);
    }

    // {max_retries, sleep_between_retries, socket_timeout, total_timeout}
    const ERL_NIF_TERM* policy = NULL;
    int policy_length;
    long max_retries = 0;
    long sleep_between_retries = 0;
    long socket_timeout = 30000;
    long total_timeout = 1000;
    if (!enif_get_tuple(env, argv[5], &policy_length, &policy) || policy_length != 4) {
        return enif_make_badarg(env);
    }
    enif_get_long(env, policy[0], &max_retries);
    enif_get_long(env, policy[1], &sleep_between_retries);
    enif_get_long(env, policy[2], &socket_timeout);
    enif_get_long(env, policy[3], &total_timeout);

    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = atom_ok;
        msg = enif_make_string(env, "put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    as_error err;
    as_key key;
    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());

    vector<as_cdt_ctx*> ctx_vec;
    as_operations ops;
    as_map_policy put_mode;
    // as_map_policy_set(&put_mode, AS_MAP_UNORDERED, AS_MAP_UPDATE);
    as_map_policy_set(&put_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);

    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        ErlNifBinary bin_bin;
        string bin_str;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;
        // as_bytes as_bytes_val;
        unsigned int ts_length;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2) {
            return enif_make_badarg(env);
        }

        if (!enif_inspect_binary(env, tuple[0], &bin_bin)) {
            return enif_make_badarg(env);
        }
        bin_str.assign((const char*)bin_bin.data, bin_bin.size);

        if (!enif_is_list(env, tuple[1]) || !enif_get_list_length(env, tuple[1], &ts_length)) {
            return enif_make_badarg(env);
        }
        auto ts_list = tuple[1];
        as_operations_inita(&ops, ts_length + 1);
        if (ttl != 0) {
            ops.ttl = ttl;
        } else {
            ops.ttl = -2;
        }
        uint opnum = 0;
        ErlNifBinary bin_key, bin_val;
        as_string key_str, subkey1, subkey2, subkey3;
        as_bytes subval1;
        as_integer subval2, subval3;
        string fcap_key, valuesk, valuesk1, valuesk2;
        long i64;
        for (uint ts_i = 0; ts_i < ts_length; ts_i++) {
            ERL_NIF_TERM ts_head;
            ERL_NIF_TERM ts_tail;
            if (!enif_get_list_cell(env, ts_list, &ts_head, &ts_tail)) {
                break;
            }

            if (opnum == 0) {
                // getting fcap key
                ctx_vec.push_back(as_cdt_ctx_create(1));
                if (enif_inspect_binary(env, ts_head, &bin_key)) {
                    fcap_key.assign((const char*)bin_key.data, bin_key.size);
                    as_string_init(&key_str, (char*)fcap_key.c_str(), false);
                    as_cdt_ctx_add_map_key_create(ctx_vec.back(), (as_val*)&key_str, AS_MAP_KEY_ORDERED);
                }
                opnum++;
            } else if (opnum == 1) {
                // getting fcap value
                if (enif_inspect_binary(env, ts_head, &bin_val)) {
                    valuesk = "value";
                    as_string_init(&subkey1, (char*)valuesk.c_str(), false);
                    as_bytes_inita(&subval1, bin_val.size);
                    as_bytes_set(&subval1, 0, bin_val.data, bin_val.size);
                    as_operations_map_put(&ops, bin_str.c_str(), ctx_vec.back(), &put_mode, (as_val*)&subkey1, (as_val*)&subval1);
                }
                opnum++;

            } else if (opnum == 2) {
                // getting subkey ttl
                if (enif_get_int64(env, ts_head, &i64)) {
                    valuesk1 = "ttl";
                    as_string_init(&subkey2, (char*)valuesk1.c_str(), false);
                    as_integer_init(&subval2, i64);
                    as_operations_map_put(&ops, bin_str.c_str(), ctx_vec.back(), &put_mode, (as_val*)&subkey2, (as_val*)&subval2);
                }
                opnum = 0;
                // subkey write time
                auto now = chrono::system_clock::now().time_since_epoch();
                long wt = chrono::duration_cast<chrono::seconds>(now).count();
                valuesk2 = "wt";
                as_string_init(&subkey3, (char*)valuesk2.c_str(), false);
                as_integer_init(&subval3, wt);
                as_operations_map_put(&ops, bin_str.c_str(), ctx_vec.back(), &put_mode, (as_val*)&subkey3, (as_val*)&subval3);
            } else {
                break;
            }

            ts_list = ts_tail;
        }
    }

    // const as_policy_operate * policy;
    as_policy_operate p;
    as_policy_operate_init(&p);
    p.ttl = ttl;
    p.base.max_retries = max_retries;
    p.base.sleep_between_retries = sleep_between_retries;
    p.base.socket_timeout = socket_timeout;
    p.base.total_timeout = total_timeout;

    SyncOperationCounter conn_counter;

    auto op_rc = aerospike_key_operate(as, &err, &p, &key, &ops, NULL);

    as_operations_destroy(&ops);
    as_key_destroy(&key);
    // destroy all contexts
    for (as_cdt_ctx* pctx : ctx_vec) {
        as_cdt_ctx_destroy(pctx);
    }

    if (op_rc != AEROSPIKE_OK) {
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, msg);
        return enif_make_tuple2(env, atom_error, error_tuple);
    }

    msg = enif_make_string(env, "put", ERL_NIF_UTF8);
    return enif_make_tuple2(env, atom_ok, msg);
}

ERL_NIF_TERM aspike_nif_cdt_get_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    string name_space, aspk_set, aspk_key;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    // {max_retries, sleep_between_retries, socket_timeout, total_timeout}
    const ERL_NIF_TERM* policy = NULL;
    int policy_length;
    long max_retries = 0;
    long sleep_between_retries = 0;
    long socket_timeout = 30000;
    long total_timeout = 1000;
    if (!enif_get_tuple(env, argv[3], &policy_length, &policy) || policy_length != 4) {
        return enif_make_badarg(env);
    }
    enif_get_long(env, policy[0], &max_retries);
    enif_get_long(env, policy[1], &sleep_between_retries);
    enif_get_long(env, policy[2], &socket_timeout);
    enif_get_long(env, policy[3], &total_timeout);

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());
    as_policy_read p;
    as_policy_read_init(&p);
    p.base.max_retries = max_retries;
    p.base.sleep_between_retries = sleep_between_retries;
    p.base.socket_timeout = socket_timeout;
    p.base.total_timeout = total_timeout;

    SyncOperationCounter conn_counter;

    if (aerospike_key_get(as, &err, &p, &key, &p_rec) != AEROSPIKE_OK) {
        if (p_rec != NULL) {
            as_record_destroy(p_rec);
        }
        as_key_destroy(&key);
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, msg);
        return enif_make_tuple2(env, atom_error, error_tuple);
    }

    as_key_destroy(&key);
    if (p_rec == NULL) {
        rc = atom_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, enif_make_int(env, ASPIKE_NIF_OK), enif_make_int(env, AEROSPIKE_ERR), msg);
        return enif_make_tuple2(env, rc, error_tuple);
    }

    msg = aspike_dump_cdt_records(env, p_rec);
    rc = atom_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_cdt_select_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    string name_space, aspk_set, aspk_key;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    // argv[3] is a list of bin name binaries
    unsigned int bin_list_len;
    if (!enif_get_list_length(env, argv[3], &bin_list_len) || bin_list_len == 0) {
        return enif_make_badarg(env);
    }

    vector<string> bin_name_strs;
    bin_name_strs.reserve(bin_list_len);
    ERL_NIF_TERM list = argv[3];
    ERL_NIF_TERM head;
    for (unsigned int i = 0; i < bin_list_len; i++) {
        enif_get_list_cell(env, list, &head, &list);
        ErlNifBinary bin;
        if (!enif_inspect_binary(env, head, &bin)) {
            return enif_make_badarg(env);
        }
        bin_name_strs.emplace_back((const char*)bin.data, bin.size);
    }

    vector<const char*> bins(bin_list_len + 1);
    for (unsigned int i = 0; i < bin_list_len; i++) {
        bins[i] = bin_name_strs[i].c_str();
    }
    bins[bin_list_len] = NULL;

    // argv[4] is policy tuple {max_retries, sleep_between_retries, socket_timeout, total_timeout}
    const ERL_NIF_TERM* policy = NULL;
    int policy_length;
    long max_retries = 0;
    long sleep_between_retries = 0;
    long socket_timeout = 30000;
    long total_timeout = 1000;
    if (!enif_get_tuple(env, argv[4], &policy_length, &policy) || policy_length != 4) {
        return enif_make_badarg(env);
    }
    enif_get_long(env, policy[0], &max_retries);
    enif_get_long(env, policy[1], &sleep_between_retries);
    enif_get_long(env, policy[2], &socket_timeout);
    enif_get_long(env, policy[3], &total_timeout);

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());
    as_policy_read p;
    as_policy_read_init(&p);
    p.base.max_retries = max_retries;
    p.base.sleep_between_retries = sleep_between_retries;
    p.base.socket_timeout = socket_timeout;
    p.base.total_timeout = total_timeout;

    SyncOperationCounter conn_counter;

    if (aerospike_key_select(as, &err, &p, &key, bins.data(), &p_rec) != AEROSPIKE_OK) {
        if (p_rec != NULL) {
            as_record_destroy(p_rec);
        }
        as_key_destroy(&key);
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, msg);
        return enif_make_tuple2(env, atom_error, error_tuple);
    }

    as_key_destroy(&key);
    if (p_rec == NULL) {
        rc = atom_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, enif_make_int(env, ASPIKE_NIF_OK), enif_make_int(env, AEROSPIKE_ERR), msg);
        return enif_make_tuple2(env, rc, error_tuple);
    }

    msg = aspike_dump_cdt_records(env, p_rec);
    rc = atom_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key, bin_name;
    string name_space, aspk_set, aspk_key, bin_str;
    unsigned int length;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    if (!enif_inspect_binary(env, argv[3], &bin_name)) {
        return enif_make_badarg(env);
    }
    bin_str.assign((const char*)bin_name.data, bin_name.size);

    ERL_NIF_TERM list = argv[4];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    as_arraylist remove_list;
    as_arraylist_init(&remove_list, length, length);

    as_error err;
    as_key key;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());

    as_operations ops;
    as_operations_inita(&ops, 1);
    ops.ttl = AS_RECORD_NO_CHANGE_TTL;  // Preserve existing record TTL (-2)
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
            subkey_str.assign((const char*)subkey_term.data, subkey_term.size);
            as_arraylist_append_str(&remove_list, (char*)subkey_str.c_str());
            subkeys_num++;
        }
        list = tail;
    }
    if (subkeys_num == length) {
        as_operations_add_map_remove_by_key_list(&ops, bin_str.c_str(), (as_list*)&remove_list, AS_MAP_RETURN_NONE);
    }
    as_arraylist_destroy(&remove_list);

    SyncOperationCounter conn_counter;

    if (aerospike_key_operate(as, &err, NULL, &key, &ops, NULL) != AEROSPIKE_OK) {
        as_operations_destroy(&ops);
        return make_nif_error(env, int(err.code), err.message);
    }
    as_operations_destroy(&ops);
    ERL_NIF_TERM msg = enif_make_string(env, "keys_deleted", ERL_NIF_UTF8);
    return enif_make_tuple2(env, atom_ok, msg);
}

ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_batch_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_name;
    string name_space, aspk_set, aspk_key, bin_str;
    unsigned int length;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_name)) {
        return enif_make_badarg(env);
    }
    bin_str.assign((const char*)bin_name.data, bin_name.size);

    ERL_NIF_TERM key_subkeys_list = argv[3];
    if (!enif_is_list(env, key_subkeys_list) || !enif_get_list_length(env, key_subkeys_list, &length)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    vector<string> bin_str_list(length);
    vector<vector<string>> skeys_lst;
    vector<as_batch_write_record*> abwrs(length);
    vector<as_operations> wopsl(length);
    vector<as_arraylist> rval(length);

    as_batch_records recs;
    as_batch_records_inita(&recs, length);

    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        ErlNifBinary bin_bin;

        if (!enif_get_list_cell(env, key_subkeys_list, &head, &tail)) {
            break;
        }
        const ERL_NIF_TERM* ksk_tuple = NULL;
        int ksk_length;
        if (!enif_get_tuple(env, head, &ksk_length, &ksk_tuple) || ksk_length != 2) {
            return enif_make_badarg(env);
        }

        if (!enif_inspect_binary(env, ksk_tuple[0], &bin_bin)) {
            return enif_make_badarg(env);
        }
        bin_str_list[i].assign((const char*)bin_bin.data, bin_bin.size);

        unsigned int ts_length;
        if (!enif_is_list(env, ksk_tuple[1]) || !enif_get_list_length(env, ksk_tuple[1], &ts_length)) {
            return enif_make_badarg(env);
        }

        abwrs[i] = as_batch_write_reserve(&recs);
        as_key_init_str(&(abwrs[i]->key), name_space.c_str(), aspk_set.c_str(), bin_str_list[i].c_str());

        auto ts_list = ksk_tuple[1];
        vector<string> bin_str_sk_list(ts_length);
        as_arraylist_init(&(rval[i]), ts_length, ts_length);
        for (uint j = 0; j < ts_length; j++) {
            ERL_NIF_TERM skl_head;
            ERL_NIF_TERM skl_tail;
            ErlNifBinary skl_bin_bin;
            if (!enif_get_list_cell(env, ts_list, &skl_head, &skl_tail)) {
                break;
            }

            if (!enif_inspect_binary(env, skl_head, &skl_bin_bin)) {
                return enif_make_badarg(env);
            }
            bin_str_sk_list[j].assign((const char*)skl_bin_bin.data, skl_bin_bin.size);
            as_arraylist_append_str(&(rval[i]), (char*)bin_str_sk_list[j].c_str());

            ts_list = skl_tail;
        }
        skeys_lst.push_back(bin_str_sk_list);
        as_operations_inita(&(wopsl[i]), 1);
        as_operations_add_map_remove_by_key_list(&(wopsl[i]), bin_str.c_str(), (as_list*)&(rval[i]), AS_MAP_RETURN_NONE);
        wopsl[i].ttl = AS_RECORD_CLIENT_DEFAULT_TTL;
        abwrs[i]->ops = &(wopsl[i]);

        key_subkeys_list = tail;
    }

    SyncOperationCounter conn_counter;

    as->config.policies.batch_write.ttl = 1000;
    as_error err;
    as_status status = aerospike_batch_write(as, &err, NULL, &recs);

    vector<ERL_NIF_TERM> erl_list;
    for (auto aitr : abwrs) {
        erl_list.push_back(enif_make_int(env, aitr->result));
    }
    auto opsl = enif_make_list_from_array(env, erl_list.data(), erl_list.size());

    for (auto vitr : wopsl) {
        as_operations_destroy(&vitr);
    }

    as_batch_records_destroy(&recs);
    if (status != AEROSPIKE_OK) {
        auto nifErrorCode = enif_make_int(env, ASPIKE_NIF_OK);
        auto aspikeErrorCode = enif_make_int(env, err.code);
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        ERL_NIF_TERM error_tuple = enif_make_tuple3(env, nifErrorCode, aspikeErrorCode, msg);
        return enif_make_tuple2(env, atom_error, error_tuple);
    } else {
        rc = atom_ok;
        return enif_make_tuple2(env, rc, opsl);
    }
}

ERL_NIF_TERM aspike_nif_segment_tag_get_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key, bin_columns;
    string name_space, aspk_set, aspk_key, aspk_columns;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    if (!enif_inspect_binary(env, argv[3], &bin_columns)) {
        return enif_make_badarg(env);
    }
    aspk_columns.assign((const char*)bin_columns.data, bin_columns.size);

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    const char* bins[] = {aspk_columns.c_str(), NULL};

    SyncOperationCounter conn_counter;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());

    if (aerospike_key_select(as, &err, NULL, &key, bins, &p_rec) != AEROSPIKE_OK) {
        if (p_rec) as_record_destroy(p_rec);
        return make_nif_error(env, int(err.code), err.message);
    }

    if (p_rec == NULL) {
        return make_nif_error(env, int(AEROSPIKE_ERR), "NULL p_rec");
    }

    as_bin_value* val = as_record_get(p_rec, bins[0]);

    if (val == NULL) {
        as_record_destroy(p_rec);
        return make_nif_error(env, int(AEROSPIKE_ERR), "NULL val - internal error");
    }

    if (as_val_type(val) != AS_STRING && as_val_type(val) != AS_MAP) {
        as_record_destroy(p_rec);
        return make_nif_error(env, int(AEROSPIKE_ERR), "Non-string or non-map bin - internal error");
    }

    ERL_NIF_TERM res;

    if (as_val_type(val) == AS_STRING) {
        uint8_t* bin_as_str = (uint8_t*)val->string.value;
        auto len = val->string.len;

        unsigned char* val_data;
        val_data = enif_make_new_binary(env, len, &res);
        memcpy(val_data, bin_as_str, len);
    } else if (as_val_type(val) == AS_MAP) {
        as_map* amap = (as_map*)val;
        uint32_t size = as_map_size(amap);
        ERL_NIF_TERM keys[size];
        ERL_NIF_TERM vals[size];

        as_orderedmap_iterator it;
        as_orderedmap_iterator_init(&it, (as_orderedmap*)amap);

        uint32_t idx = 0;
        while (as_orderedmap_iterator_has_next(&it)) {
            as_pair* pair = as_pair_fromval(as_orderedmap_iterator_next(&it));
            keys[idx] = aspike_get_binary_from_asval(env, as_pair_1(pair));
            vals[idx] = aspike_get_binary_from_asval(env, as_pair_2(pair));
            idx++;
        }
        as_orderedmap_iterator_destroy(&it);

        ERL_NIF_TERM map_term;
        enif_make_map_from_arrays(env, keys, vals, size, &map_term);
        res = map_term;
    }

    rc = atom_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, res);
}

ERL_NIF_TERM aspike_nif_key_select_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    unsigned int length = 0;
    unsigned int i = 0;
    as_record* p_rec = NULL;

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;

    if (length == 0) {
        msg = enif_make_list(env, 0);  // empty list
        rc = atom_ok;
        return enif_make_tuple2(env, rc, msg);
    }

    const char* bins[MAX_BINS_NUMBER];
    for (; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (!enif_get_string(env, head, bin, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
            break;
        }
        bins[i] = (const char*)cf_strdup(bin);
        list = tail;
    }
    bins[i] = NULL;

    as_error err;
    as_key key;
    as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_select(as, &err, NULL, &key, bins, &p_rec) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    msg = aspike_dump_records(env, p_rec);
    rc = atom_ok;
    for (uint j = 0; j < i; j++) {
        cf_free((void*)bins[j]);
    }
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_binary_get_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    string name_space, aspk_set, aspk_key;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());

    if (aerospike_key_get(as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
        if (p_rec != NULL) {
            as_record_destroy(p_rec);
        }
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    if (p_rec == NULL) {
        rc = atom_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    msg = aspike_dump_binary_records(env, p_rec);
    rc = atom_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_key_get_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_get(as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
        if (p_rec != NULL) {
            as_record_destroy(p_rec);
        }
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    if (p_rec == NULL) {
        rc = atom_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    msg = aspike_dump_records(env, p_rec);
    rc = atom_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_key_exists_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    as_key_init_str(&key, name_space, set, key_str);
    int as_rc = aerospike_key_exists(as, &err, NULL, &key, &p_rec);

    if (as_rc != AEROSPIKE_OK) {
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, atom_error, msg);
    }
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, atom_ok, atom_true);
}

ERL_NIF_TERM aspike_nif_key_inc_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    unsigned int length;

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = atom_ok;
        msg = enif_make_string(env, "key_inc", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    as_error err;
    as_key key;
    as_key_init_str(&key, name_space, set, key_str);

    as_operations ops;
    as_operations_inita(&ops, length);

    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        long val = 0;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2) {
            return enif_make_badarg(env);
        }
        if (!enif_get_string(env, tuple[0], bin, MAX_SET_SIZE, ERL_NIF_UTF8)) {
            return enif_make_badarg(env);
        }
        if (!enif_get_long(env, tuple[1], &val)) {
            return enif_make_badarg(env);
        }
        as_operations_add_incr(&ops, bin, val);
        list = tail;
    }

    if (aerospike_key_operate(as, &err, NULL, &key, &ops, NULL) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = atom_ok;
        msg = enif_make_string(env, "key_inc", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_key_generation_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;

    as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_get(as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    if (p_rec == NULL) {
        rc = atom_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    ERL_NIF_TERM keys[2];
    ERL_NIF_TERM vals[2];
    keys[0] = enif_make_string(env, "gen", ERL_NIF_UTF8);
    vals[0] = enif_make_uint64(env, p_rec->gen);
    keys[1] = enif_make_string(env, "ttl", ERL_NIF_UTF8);
    vals[1] = enif_make_uint64(env, p_rec->ttl);
    enif_make_map_from_arrays(env, keys, vals, 2, &msg);
    rc = atom_ok;
    as_record_destroy(p_rec);
    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_key_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    unsigned int length;

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    // enif_get_list_length(env, *val, &len);
    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = atom_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    as_error err;
    as_key key;
    as_record rec;

    as_key_init_str(&key, name_space, set, key_str);
    as_record_inita(&rec, length);

    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        long val = 0;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2) {
            return enif_make_badarg(env);
        }
        if (!enif_get_string(env, tuple[0], bin, MAX_SET_SIZE, ERL_NIF_UTF8)) {
            return enif_make_badarg(env);
        }
        if (!enif_get_long(env, tuple[1], &val)) {
            return enif_make_badarg(env);
        }
        as_record_set_int64(&rec, bin, val);
        list = tail;
    }

    if (aerospike_key_put(as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = atom_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_binary_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    unsigned int length;
    string name_space, aspk_set, aspk_key;
    long ttl;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    if (!enif_get_long(env, argv[4], &ttl)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = atom_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    as_error err;
    as_key key;
    as_record rec;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());
    as_record_inita(&rec, length);
    rec.ttl = ttl;
    long ret_val = 0;

    vector<as_bytes*> bin_vec;
    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        ErlNifBinary bin_bin, bin_val;
        string bin_str;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;
        // as_bytes as_bytes_val;
        unsigned int ts_length;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2) {
            return enif_make_badarg(env);
        }

        if (!enif_inspect_binary(env, tuple[0], &bin_bin)) {
            return enif_make_badarg(env);
        }
        bin_str.assign((const char*)bin_bin.data, bin_bin.size);

        if (enif_inspect_binary(env, tuple[1], &bin_val)) {
            bin_vec.push_back(as_bytes_new(bin_val.size));
            as_bytes* bytes_v = bin_vec.back();
            as_bytes_set(bytes_v, 0, (const uint8_t*)bin_val.data, bin_val.size);
            if (!as_record_set_bytes(&rec, bin_str.c_str(), bytes_v)) {
                as_bytes_destroy(bytes_v);
                ret_val = 1;
            }
        } else {
            if (enif_is_number(env, tuple[1])) {
                long i64;
                if (enif_get_int64(env, tuple[1], &i64)) {
                    if (!as_record_set_int64(&rec, bin_str.c_str(), i64)) {
                        ret_val = 1;
                    }
                }
            } else if (enif_is_atom(env, tuple[1])) {  // every atom is considering as undefined to delete this binary
                if (!as_record_set_nil(&rec, bin_str.c_str())) {
                    ret_val = 1;
                }
            } else {
                if (!enif_is_list(env, tuple[1]) || !enif_get_list_length(env, tuple[1], &ts_length)) {
                    return enif_make_badarg(env);
                }
                // expecting list of integers
                auto ts_list = tuple[1];
                // as_list* as_list_of_ints = (as_list *)as_arraylist_new((uint32_t)ts_length, 0);
                as_arraylist* as_list_of_ints = as_arraylist_new((uint32_t)ts_length, 0);
                for (uint ts_i = 0; ts_i < ts_length; ts_i++) {
                    ERL_NIF_TERM ts_head;
                    ERL_NIF_TERM ts_tail;
                    long i64;
                    if (!enif_get_list_cell(env, ts_list, &ts_head, &ts_tail)) {
                        break;
                    }
                    if (enif_get_int64(env, ts_head, &i64)) {
                        as_arraylist_append_int64(as_list_of_ints, i64);
                    }
                    ts_list = ts_tail;
                }
                ((as_val*)as_list_of_ints)->type = AS_LIST;
                if (!as_record_set_list(&rec, bin_str.c_str(), (as_list*)as_list_of_ints)) {
                    as_list_destroy((as_list*)as_list_of_ints);
                    ret_val = 1;
                }
            }
        }
        list = tail;
    }
    if (!ret_val) {
        // destroy heap record
    }

    if (aerospike_key_put(as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = atom_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
    }
    for (unsigned int i = 0; i < bin_vec.size(); i++) {
        as_bytes_destroy(bin_vec[i]);
    }

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_binary_remove_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    unsigned int length;
    string name_space, aspk_set, aspk_key;
    long ttl;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
        return enif_make_badarg(env);
    }

    if (!enif_get_long(env, argv[4], &ttl)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = atom_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    as_error err;
    as_key key;
    as_record rec;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());
    as_record_inita(&rec, length);
    rec.ttl = ttl;
    long ret_val = 0;

    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        ErlNifBinary bin_bin;
        string bin_str;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if (!enif_inspect_binary(env, head, &bin_bin)) {
            return enif_make_badarg(env);
        }
        bin_str.assign((const char*)bin_bin.data, bin_bin.size);

        if (!as_record_set_nil(&rec, bin_str.c_str())) {
            ret_val = 1;
        }
    }

    if (!ret_val) {
        // destroy heap record
    }

    if (aerospike_key_put(as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = atom_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_cdt_expire_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary bin_ns, bin_set, bin_key;
    string name_space, aspk_set, aspk_key;
    long ttl;

    return enif_make_tuple2(env, atom_error, enif_make_string(env, "method not completed", ERL_NIF_UTF8));

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
        return enif_make_badarg(env);
    }
    name_space.assign((const char*)bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
        return enif_make_badarg(env);
    }
    aspk_set.assign((const char*)bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
        return enif_make_badarg(env);
    }
    aspk_key.assign((const char*)bin_key.data, bin_key.size);

    if (!enif_get_long(env, argv[3], &ttl)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;

    as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());

    as_cdt_ctx ctx;
    as_cdt_ctx_inita(&ctx, 1);
    as_operations ops;
    as_operations_inita(&ops, 1);
    as_map_policy put_mode;
    as_map_policy_set(&put_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);

    as_cdt_ctx_add_map_key(&ctx, (as_val*)&as_cmp_wildcard);
    //as_string key_main;
    //string main_key = "campaign.333";
    //as_string_init(&key_main, (char*)main_key.c_str(), false);
    //as_cdt_ctx_add_map_key(&ctx, (as_val*)&key_main);

    as_string key_ttl;
    string ttl_key = "3333";
    string bin_str = "fcap_map";
    as_string_init(&key_ttl, (char*)ttl_key.c_str(), false);
    //as_cdt_ctx_add_map_key(&ctx, (as_val*)&key_ttl);
    as_integer asv_begin, asv_end;
    as_integer_init(&asv_begin, 0);
    as_integer_init(&asv_end, ttl);
    as_operations_map_remove_by_value_range(&ops, bin_str.c_str(), &ctx, (as_val*)&asv_begin, (as_val*)&asv_end, AS_MAP_RETURN_COUNT);
    //as_operations_map_remove_by_value(&ops, bin_str.c_str(), &ctx, (as_val*)&asv_begin, AS_MAP_RETURN_COUNT);
    //as_operations_map_remove_by_key(&ops, bin_str.c_str(), &ctx, (as_val*)&key_ttl, AS_MAP_RETURN_NONE);
    //as_operations_map_get_by_value_range(&ops, bin_str.c_str(), &ctx, (as_val*)&asv_begin, (as_val*)&asv_end, AS_MAP_RETURN_NONE);
    //as_operations_map_remove_by_value(&ops, bin_str.c_str(), &ctx, (as_val*)&key_ttl, AS_MAP_RETURN_COUNT);

    // working code delete by key lists
    /*string bin_str = "fcap_map";
    as_operations ops;
    as_operations_inita(&ops, 1);
    as_map_policy put_mode;
    as_map_policy_set(&put_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);

    as_arraylist remove_list;
    as_arraylist_init(&remove_list, 2, 2);
    as_arraylist_append_str(&remove_list, "campaign.222");
    as_arraylist_append_str(&remove_list, "campaign.444");

    //as_operations_add_map_remove_by_key_list(&ops, bin_str.c_str(), (as_list*)&remove_list, AS_MAP_RETURN_COUNT);
    as_operations_add_map_remove_by_key_list(&ops, bin_str.c_str(), (as_list*)&remove_list, AS_MAP_RETURN_NONE);
    as_arraylist_destroy(&remove_list);*/

    if (aerospike_key_operate(as, &err, NULL, &key, &ops, NULL) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = atom_ok;
        msg = enif_make_string(env, "done", ERL_NIF_UTF8);
    }
    as_operations_destroy(&ops);

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_key_remove_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;

    as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_remove(as, &err, NULL, &key) != AEROSPIKE_OK) {
        rc = atom_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = atom_ok;
        msg = enif_make_string(env, "key_remove", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_a_key_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    char bin[AS_BIN_NAME_MAX_SIZE];
    long val;
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    long n;

    if (!enif_get_string(env, argv[0], bin, AS_USER_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_long(env, argv[1], &val)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[3], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[4], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_long(env, argv[5], &n)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record rec;

    as_key_init_str(&key, name_space, set, key_str);
    as_record_inita(&rec, 1);
    as_record_set_int64(&rec, bin, val);

    struct timespec thread_start, thread_done;
    struct timespec real_start, real_done;

    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &thread_start);
    clock_gettime(CLOCK_REALTIME, &real_start);

    for (uint i = 0; i < n; i++) {
        if (aerospike_key_put(as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
            rc = atom_error;
            msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
            return enif_make_tuple2(env, rc, msg);
        }
    }
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &thread_done);
    clock_gettime(CLOCK_REALTIME, &real_done);
    // Convert to microseconds
    ErlNifSInt64 thread_tspent = (thread_done.tv_sec - thread_start.tv_sec) * 1000000 + (thread_done.tv_nsec - thread_start.tv_nsec) / 1000;
    ErlNifSInt64 real_tspent = (real_done.tv_sec - real_start.tv_sec) * 1000000 + (real_done.tv_nsec - real_start.tv_nsec) / 1000;

    rc = atom_ok;

    ERL_NIF_TERM keys[3];
    ERL_NIF_TERM vals[3];
    keys[0] = enif_make_string(env, "repetitions", ERL_NIF_UTF8);
    vals[0] = enif_make_uint64(env, n);
    keys[1] = enif_make_string(env, "CLOCK_THREAD_CPUTIME_ID", ERL_NIF_UTF8);
    vals[1] = enif_make_double(env, thread_tspent / n);  // from micro to mille seconds
    keys[2] = enif_make_string(env, "CLOCK_REALTIME", ERL_NIF_UTF8);
    vals[2] = enif_make_double(env, real_tspent / n);  // from micro to mille seconds
    enif_make_map_from_arrays(env, keys, vals, 3, &msg);
    return enif_make_tuple2(env, rc, msg);
}

ERL_NIF_TERM aspike_nif_map_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    static aerospike* as = get_aerospike();

    ErlNifBinary erl_namespace, erl_set_name, erl_record_name, erl_bin_name;
    if (!enif_inspect_binary(env, argv[0], &erl_namespace)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &erl_set_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &erl_record_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &erl_bin_name)) {
        return enif_make_badarg(env);
    }
    if (!enif_is_number(env, argv[4])) {
        return enif_make_badarg(env);
    }
    ERL_NIF_TERM erl_map = argv[5];
    if (!enif_is_map(env, erl_map)) {
        return enif_make_badarg(env);
    }
    // Get the map size for initialization
    size_t map_size;
    if (!enif_get_map_size(env, erl_map, &map_size)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM return_data;
    if (!is_connected(env, &return_data)) return return_data;

    string name_space = string((const char*)erl_namespace.data, erl_namespace.size);
    string set_name = string((const char*)erl_set_name.data, erl_set_name.size);
    string record_name((const char*)erl_record_name.data, erl_record_name.size);
    string bin_name((const char*)erl_bin_name.data, erl_bin_name.size);
    long ttl;
    enif_get_long(env, argv[4], &ttl);

    as_key record_key;
    as_key_init_str(&record_key, name_space.c_str(), set_name.c_str(), record_name.c_str());

    as_record rec;
    as_record_inita(&rec, 1);
    rec.ttl = ttl;

    // Create an ordered map to hold the Erlang map data
    as_orderedmap as_map_data;
    as_orderedmap_init(&as_map_data, (uint32_t)map_size);

    // Iterate over the Erlang map and convert each key-value pair
    ErlNifMapIterator iter;
    if (enif_map_iterator_create(env, erl_map, &iter, ERL_NIF_MAP_ITERATOR_FIRST)) {
        ERL_NIF_TERM key, value;

        while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
            // Convert key to as_val - keys can be atoms, strings (lists), or binaries
            as_val* as_key = NULL;
            ErlNifBinary key_binary;
            char atom_buffer[256];
            char string_buffer[256];

            if (enif_inspect_binary(env, key, &key_binary)) {
                // Key is a binary (<<"key">>)
                char* key_str = (char*)malloc(key_binary.size + 1);
                memcpy(key_str, key_binary.data, key_binary.size);
                key_str[key_binary.size] = '\0';
                as_key = (as_val*)as_string_new(key_str, true);  // true means Aerospike will free the memory
            } else if (enif_is_atom(env, key) && enif_get_atom(env, key, atom_buffer, sizeof(atom_buffer), ERL_NIF_UTF8)) {
                // Key is an atom ('key')
                as_key = (as_val*)as_string_new_strdup(atom_buffer);  // strdup creates a copy
            } else if (enif_get_string(env, key, string_buffer, sizeof(string_buffer), ERL_NIF_UTF8)) {
                // Key is a string ("key") - character list
                as_key = (as_val*)as_string_new_strdup(string_buffer);  // strdup creates a copy
            } else {
                // Unsupported key type - keys must be atoms, strings, or binaries
                enif_map_iterator_destroy(env, &iter);
                as_orderedmap_destroy(&as_map_data);
                as_record_destroy(&rec);
                as_key_destroy(&record_key);
                return enif_make_badarg(env);
            }

            // Convert value to as_val - only handle binary/string data as expected by binary_get_sync
            as_val* as_value = NULL;
            ErlNifBinary value_binary;

            if (enif_inspect_binary(env, value, &value_binary)) {
                // Value is a binary - store as bytes
                uint8_t* value_data = (uint8_t*)malloc(value_binary.size);
                memcpy(value_data, value_binary.data, value_binary.size);
                as_value = (as_val*)as_bytes_new_wrap(value_data, value_binary.size, true);
            } else {
                // Unsupported value type - only binary/string values are supported
                as_val_destroy(as_key);
                enif_map_iterator_destroy(env, &iter);
                as_orderedmap_destroy(&as_map_data);
                as_record_destroy(&rec);
                as_key_destroy(&record_key);
                return enif_make_badarg(env);
            }

            // Add the key-value pair to the ordered map
            if (as_orderedmap_set(&as_map_data, as_key, as_value) != 0) {
                // Failed to set - clean up and return error
                as_val_destroy(as_key);
                as_val_destroy(as_value);
                enif_map_iterator_destroy(env, &iter);
                as_orderedmap_destroy(&as_map_data);
                as_record_destroy(&rec);
                as_key_destroy(&record_key);
                return enif_make_badarg(env);
            }

            // Move to next pair
            if (!enif_map_iterator_next(env, &iter)) {
                break;
            }
        }

        enif_map_iterator_destroy(env, &iter);
    }

    // Set the map in the record
    bool map_set_success = as_record_set_map(&rec, bin_name.c_str(), (as_map*)&as_map_data);

    if (!map_set_success) {
        as_orderedmap_destroy(&as_map_data);
        as_record_destroy(&rec);
        as_key_destroy(&record_key);
        return enif_make_badarg(env);
    }

    as_error err;
    as_status status = aerospike_key_put(as, &err, NULL, &record_key, &rec);

    // Clean up the ordered map after the put operation
    as_orderedmap_destroy(&as_map_data);
    as_record_destroy(&rec);
    as_key_destroy(&record_key);

    if (status != AEROSPIKE_OK) {
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
        return_data = enif_make_tuple2(env, atom_ok, atom_done);
    }

    return return_data;
}
