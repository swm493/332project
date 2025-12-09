# 332project

## Requirement
- java 1.8 (JDK 8u202)
- scala 3.3.7
- sbt 1.11.7
## How To Build
1. use this command to compile executable java file.
```bash
sbt clean assembly
```
2. move to where compiled file exist.
```bash
$ cd target/scala-3.3.7/
$ ls
332project-assembly-0.1.0-SNAPSHOT.jar
```
3. move this file to where master and worker nodes exist.
```bash
scp -P [port] 332project-assembly-0.1.0-SNAPSHOT.jar [master/worker address]
```
4. excute program with these command.
```bash
# master
java -cp 332project-assembly-0.1.0-SNAPSHOT.jar master.MasterApp <num of workers> [-d | --debug]
```
```bash
# worker
java -cp 332project-assembly-0.1.0-SNAPSHOT.jar worker.WorkerApp <master IP:port> -I <input dir1> [<input dir2> ...] -O <output dir> [-d | --debug]
```
you can use debug mode with `-d` to see the more specific log in master and worker's process.

## :pushpin: Description

TBD

## :sparkles: Skills & Tech Stack

1. 프로젝트 관리 : Git bash / Git Kraken (version control)
2. 커뮤니케이션 : Discord, Kakaotalk, 대면 미팅

## :card_file_box: Packages

TBD

## :hammer_and_wrench: Git

1. Commit / PR 컨벤션
    - `Feat` : 새로운 기능 추가
    - `Fix` : 버그 수정
    - `Design` : 디자인 및 UI 수정
    - `Docs` : 문서 (README, 메뉴얼 등)
    - `Test` : 테스트 코드
    - `Refactor` : 코드 리팩토링 (기능 변화 없이 성능 개선)
    - `Style` : 코드 의미에 영향을 주지 않는 변경 사항
    - `Chore` : 빌드, 설정 파일
    - `Comment` : 주석 추가
2. Git 브랜치
    - `main` : 기본 브랜치
    - `develop` : 개발된 기능(feature)을 통합하는 브랜치
    - `docs` : 문서작업 브랜치
    - `feat/[issue_num]-[function name]` : 각 기능별 개발을 진행하는 브랜치
    - `bug/[issue_num]-[bug name]` : 버그 해결용 브랜치
    - `test/[name]` : 테스트용 브랜치


## :fountain_pen: Authors

- 하승재, 전재영, 채승현
