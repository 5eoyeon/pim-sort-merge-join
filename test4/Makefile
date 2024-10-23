CC = gcc
CFLAG = --std=c99
CLANG = dpu-upmem-dpurte-clang
DNR_TASKLETS = -DNR_TASKLETS=15

CPU_APP = cpu_app
APP = app
SELECT = select
SORT_DPU = sort_dpu
MERGE_DPU = merge_dpu
JOIN = join

CPU_APP_SRC = cpu_app.c
APP_SRC = app.c    
SELECT_SRC = select.c
SORT_DPU_SRC = sort_dpu.c
MERGE_DPU_SRC = merge_dpu.c
JOIN_SRC = join.c

all: $(CPU_APP) $(APP) $(SELECT) $(SORT_DPU) $(MERGE_DPU) $(JOIN)

$(CPU_APP) : $(CPU_APP_SRC)
	$(CC) -o $(CPU_APP) $(CPU_APP_SRC)

$(APP) : $(APP_SRC)
	$(CC) $(CFLAG) $(APP_SRC) -o $(APP) `dpu-pkg-config --cflags --libs dpu`

$(SELECT) : $(SELECT_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(SELECT) $(SELECT_SRC)

$(SORT_DPU) : $(SORT_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(SORT_DPU) $(SORT_DPU_SRC)

$(MERGE_DPU) : $(MERGE_DPU_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(MERGE_DPU) $(MERGE_DPU_SRC)

$(JOIN) : $(JOIN_SRC)
	$(CLANG) $(DNR_TASKLETS) -o $(JOIN) $(JOIN_SRC)

clean: 
	rm -f $(CPU_APP) $(APP) $(SELECT) $(SORT_DPU) $(MERGE_DPU) $(JOIN)
