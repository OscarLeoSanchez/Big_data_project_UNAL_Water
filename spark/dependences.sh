

apt-get update

apt-get install -y libpq-dev python3-dev build-essential

pip install -r requirements.txt

wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

curl -O https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

mv postgresql-42.5.0.jar /opt/spark/jars/

echo "spark.driver.extraClassPath /opt/spark/jars/postgresql-42.5.0.jar" >> $SPARK_HOME/conf/spark-defaults.conf



# spark-submit --jars /opt/spark/jars/postgresql-42.5.0.jar process_gold.py