# HYU CSE Capstone Software Project
한양대학교 컴퓨터소프트웨어학부 졸업프로젝트를 위한 레포지토리입니다.  
repository for [HYU CSE](http://cs.hanyang.ac.kr/) Capstone Software Project  
## Processing-in-Memory 활용 기술 개발
Processing-in-Memory 기술을 이용하여 기존에 존재하는 알고리즘의 성능 향상 방법 개발  
Enhance the performance of algorithms using Processing-in-Memory  
- Processing-in-Memory의 개념 이해  
  Understand the Concept of Processing-in-Memory  
- UPMEM-PIM의 구조 및 UPMEM-PIM SDK를 분석하여 PIM을 이용한 개발 방법 파악  
  Analyze UPMEM-PIM Architecture and SDK  
- PIM을 이용하여 sort-merge join 알고리즘의 성능 향상 방법 고안 및 구현  
  Enhance the Performance of Sort-Merge Join Algorithm Using PIM
- PIM의 유무에 따른 성능 비교  
  Compare Performance with and without PIM  
  
2024-1 ~ 2024-2  
지도교수: 강수용  
팀원: 2021019961 장서연, 2021097356 김한결

## 개발 환경
* PIM 하드웨어: V1B
* OS: Ubuntu 20.04.6 LTS
* UPMEM DPU toolchain (version 2023.2.0)

## 실행 방법  
PIM 서버 사용이 불가능한 경우, 아래 과정을 거쳐 시뮬레이터로 실행합니다.  
```bash
# python 베이스의 디버깅 툴 사용, python 3.x을 권장
$ apt install python3   

# SDK 다운로드
# https://sdk.upmem.com/
$ wget http://sdk-releases.upmem.com/2023.2.0/ubuntu_20.04/upmem-2023.2.0-Linux-x86_64.tar.gz

# extract & source the configs
$ tar xvf upmem-2023.2.0-Linux-x86_64.tar.gz
$ cd upmem-sdk

# UPMEM SDK를 위한 환경 변수 세팅
$ source upmem_env.sh

# simulator background 설정을 위해 `simulator` flag를 추가  
# functional simulator는 기본적으로 UPMEM DPU toolchain에 통합되어 있으며,
# 시스템에 UPMEM DIMM이 없으면 시뮬레이터 자동 매핑
$ source upmem_env.sh simulator

# 본 레포지토리를 clone
$ git clone https://github.com/5eoyeon/pim-sort-merge-join.git

# pim-sort-merge-join/sort-merge-join/data 내에 input 파일 위치  
# 파일명은 data1.csv, data2.csv로 입력 (예시 데이터 존재)
# generate_data.py를 통해 랜덤한 데이터를 새롭게 생성 가능   

# pim-sort-merge-join/sort-merge-join/user.h 수정
$ cd pim-sort-merge-join/sort-merge-join
$ vi user.h

# PIM을 활용한 sort-merge join 적용
# pim-sort-merge-join/sort-merge-join/run.py 실행
# 터미널에 실행 시간이 출력
$ python3 run.py
  
# pim-sort-merge-join/sort-merge-join/data 내에 output 파일 자동 생성
$ vi result.csv
```
