CC = gcc
CFLAG = --std=c99
CLANG = dpu-upmem-dpurte-clang
DNR_TASKLETS = -DNR_TASKLETS=2

APP = app
SELECT = select
SORT_DPU = sort_dpu
MERGE_DPU = merge_dpu

APP_SRC = app.c    
SELECT_SRC = select.c
SORT_DPU_SRC = sort_dpu.c
MERGE_DPU_SRC = merge_dpu.c

all: $(APP) $(SELECT) $(SORT_DPU) $(MERGE_DPU)

$(APP) : $(APP_SRC)
	$(CC) $(CFLAG) $(APP_SRC) -o $(APP) `dpu-pkg-config --cflags --libs dpu`

$(SELECT) : $(SELECT_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(SELECT) $(SELECT_SRC)

$(SORT_DPU) : $(SORT_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(SORT_DPU) $(SORT_DPU_SRC)

$(MERGE_DPU) : $(MERGE_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(MERGE_DPU) $(MERGE_DPU_SRC)

clean: 
	rm -f $(APP) $(SELECT) $(SORT_DPU) $(MERGE_DPU)
