CC = gcc
CFLAG = --std=c99
CLANG = dpu-upmem-dpurte-clang
DNR_TASKLETS = -DNR_TASKLETS=24

APP = app
SELECT = select
QUICK_SORT_DPU = quick_sort_dpu
MERGE_SORT_DPU = merge_sort_dpu
MERGE_DPU = merge_dpu
JOIN = join

APP_SRC = app.c    
SELECT_SRC = select.c
MERGE_SORT_DPU_SRC = merge_sort_dpu.c
QUICK_SORT_DPU_SRC = quick_sort_dpu.c
MERGE_DPU_SRC = merge_dpu.c
JOIN_SRC = join.c

all: $(APP) $(SELECT) $(SORT_DPU) $(MERGE_DPU) $(JOIN)

$(APP) : $(APP_SRC)
	$(CC) $(CFLAG) $(APP_SRC) -o $(APP) `dpu-pkg-config --cflags --libs dpu`

$(SELECT) : $(SELECT_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(SELECT) $(SELECT_SRC)

$(QUICK_SORT_DPU) : $(QUICK_SORT_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(QUICK_SORT_DPU) $(QUICK_SORT_DPU_SRC)

$(MERGE_SORT_DPU) : $(MERGE_SORT_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(MERGE_SORT_DPU) $(MERGE_SORT_DPU_SRC)

$(MERGE_DPU) : $(MERGE_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(MERGE_DPU) $(MERGE_DPU_SRC)

$(JOIN) : $(JOIN_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(JOIN) $(JOIN_SRC)

clean: 
	rm -f $(APP) $(SELECT) $(SORT_DPU) $(MERGE_DPU) $(JOIN)
