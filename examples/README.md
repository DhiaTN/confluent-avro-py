# Run live demo

1.Run full kafka environment
```shell script
docker-compose up -d
```
> Visit [localhost:3030](localhost:3030) to play with the nice [fast-data-dev](https://github.com/lensesio/fast-data-dev) dashboard

2.Install requirements

```shell script
pip install kafka-python confluent_avro
```
3.Run consumer app in tab 1

```shell script
python consumer.py
```

4.Run producer app in tab 2

```shell script
python producer.py
```

5.Play with the schema and the data to test different scenarios

#### Credit:
- Data used in this examples are taken from [landoop/sample-data](https://github.com/lensesio/fast-data-dev/tree/37eaebcbfc533668df46bdeffe2fee7c6f396f4f/filesystem/usr/local/share/landoop/sample-data)
