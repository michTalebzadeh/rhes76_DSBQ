from pyspark.sql import SparkSession
from pyspark import SparkContext

def main():
    # Define the Spark application name
    appName = "checkpoint_stuff"
    # Initialize Spark session and context
    spark_session = SparkSession.builder.appName(appName).getOrCreate()
    spark_context = SparkContext.getOrCreate()

    # Set the log level to ERROR to reduce verbosity
    spark_context.setLogLevel("ERROR")

    # Define the path to the input text file
    input_file_path = "gs://etcbucket/README.md" # replace it with the correct bucket name

    # Define the checkpoint directory
    checkpoint_directory = "file:///ssd/hduser/spark/tmp/checkpoint_directory"

    # Define the maximum number of retries
    max_retries = 3

    avg_word_length = Calculate_avg(spark_session, spark_context)

    # Call the function with retry logic
    avg_word_length.calculate_average_word_length(input_file_path, checkpoint_directory, max_retries)

class Calculate_avg:
    _instance = None  # Class-level variable to store the singleton instance
    def __init__(self, spark_session, spark_context):
        self.spark = spark_session
        self.sc = spark_context

    @staticmethod
    def word_length_count( word):
        return len(word), 1

    def calculate_average_word_length(self, input_file_path, checkpoint_directory, max_retries):
        retries = 0
        while retries < max_retries:
            try:
                # Load the text file as an RDD and split lines into words
                lines = self.sc.textFile(input_file_path)
                words = lines.flatMap(lambda line: line.split(" "))

                # Calculate word lengths and counts
                word_lengths_counts = words.map(self.word_length_count)

                # Checkpoint the intermediate RDD for fault tolerance
                self.sc.setCheckpointDir(checkpoint_directory)
                word_lengths_counts.checkpoint()

                # Calculate the sum of word lengths and word count
                total_lengths_counts = word_lengths_counts.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

                # Calculate the average word length
                average_word_length = total_lengths_counts[0] / total_lengths_counts[1]

                # Format the result to two decimal points
                formatted_average = "{:.2f}".format(average_word_length)

                # Display the result
                print(f"Average Word Length: {formatted_average}")

                break  # Break out of the retry loop on success

            except Exception as e:
                print(f"Error: {str(e)}")
                retries += 1
                if retries < max_retries:
                    print(f"Retrying... (Retry {retries}/{max_retries})")
                else:
                    print("Max retries reached. Exiting.")
                    break

if __name__ == "__main__":
    print("\nWorking on this code")
    main()

