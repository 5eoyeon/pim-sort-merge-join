gcc --std=c99 app.c -o app `dpu-pkg-config --cflags --libs dpu`
dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o select select.c
dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o sort_dpu sort_dpu.c