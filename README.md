# cosmos-load-test
Run Load Test Instructions

git clone https://github.com/prjindal/cosmos-load-test.git

cd cosmos-load-test

mvn clean package

export INITIAL_RESULTS_TO_SKIP=100

export TIME_TO_SLEEP_IN_MILLS=8

export WRITE_SLEEP_MILLS=8

export MASTER_KEY="<please give master key for cosmos db account>"

java -cp target/load-test-1.0-SNAPSHOT-jar-with-dependencies.jar com.adobe.cosmos.LoadTest 
