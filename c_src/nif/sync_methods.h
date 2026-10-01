#ifndef SYNC_METHODS_H
#define SYNC_METHODS_H

ERL_NIF_TERM aspike_nif_cdt_put_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_get_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_batch_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_segment_tag_get_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_select_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_select_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_binary_get_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_map_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_get_sync (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_exists_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_inc_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_generation_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_binary_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_binary_remove_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_expire_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_key_remove_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_a_key_put_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif // SYNC_METHODS_H
