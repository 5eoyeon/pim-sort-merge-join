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
<br>
<br>
## 실행 방법
PIM 서버가 없는 경우, 아래 과정을 거쳐 시뮬레이터로 실행합니다.  
<br>
* python 베이스의 디버깅 툴 사용, python 3.x을 권장
```bash
$ apt install python3
```  
<br>
* SDK 다운로드
https://sdk.upmem.com/
```bash
$ wget http://sdk-releases.upmem.com/2023.2.0/ubuntu_20.04/upmem-2023.2.0-Linux-x86_64.tar.gz
```  
<br>
* extract & source the configs
```bash
$ tar xvf upmem-2023.2.0-Linux-x86_64.tar.gz
$ cd upmem-sdk
$ ls
```  
<br>
* UPMEM SDK를 위한 환경 변수 세팅
```bash
$ source upmem_env.sh
```  
<br>
* simulator background 설정을 위해 `simulator` flag를 추가  
(functional simulator는 기본적으로 UPMEM DPU toolchain에 통합되어 있으며, 시스템에 UPMEM DIMM이 없으면 시뮬레이터 자동 매핑)
```bash
$ source upmem_env.sh simulator
```  
<br>
* 본 레포지토리를 clone
```bash
$ git clone https://github.com/5eoyeon/pim-sort-merge-join.git
```  
<br>
* pim-sort-merge-join/sort-merge-join/data 내에 input 파일 위치
  * 파일 명은 data1.csv, data2.csv로 입력 (예시 데이터 존재)
  * generate_data.py를 통해 랜덤한 데이터를 새롭게 생성 가능  
<br>
* pim을 활용한 sort-merge join 적용
  * 터미널에 실행 시간이 출력
```bash
$ python3 run.py
```  
<br>   
* pim-sort-merge-join/sort-merge-join/data 내에 output 파일 자동 생성
  * result.csv
