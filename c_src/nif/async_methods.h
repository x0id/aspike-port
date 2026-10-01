#ifndef ASYNC_METHODS_H
#define ASYNC_METHODS_H

// Async method declarations
ERL_NIF_TERM aspike_nif_cdt_put_async (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_get_async (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_async (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_delete_by_keys_batch_async (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_segment_tag_get_async (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM aspike_nif_cdt_select_async (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif // ASYNC_METHODS_H
