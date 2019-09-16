# Instructions

## Set up the environment

1. Start the "spark cluster" locally

    ```bash
    docker-compose up
    ```

2. Set up a Kaggle API Token

    https://github.com/Kaggle/kaggle-api#api-credentials

3. Download the data

    ```bash
    # get the kaggle-cli
    sudo pip install kaggle

    # check that the API config is in the right place
    ls ~/.kaggle/kaggle.json

    # download the datasets
    kaggle datasets download --unzip -p data/ olistbr/brazilian-ecommerce
    ```

4. SSH into your Spark master container

    ```bash
    docker exec -it docker-spark_master_1 /bin/bash
    ```

5. Start the REPL

    ```bash
    spark-shell
    ```

Now have a look at Spark.scala, you can copy & paste bits on the REPL and play with the data! have fun!

## UI

The Spark history server will start on localhost:8080
