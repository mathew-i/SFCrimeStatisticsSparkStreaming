from kafka import KafkaConsumer

BROKER = "localhost:9092"

def main():
    consumer = KafkaConsumer(
        'com.udacity.police.calls.new',
        bootstrap_servers=[BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer_server'
    )
    
    i=1 
    for message in consumer:
        message = message.value
        print(f'{i} - {message}')
        i+=1

if __name__ == "__main__":
    main()