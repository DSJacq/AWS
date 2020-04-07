# Create function that reads the number of records on each partition
def printRecordsPerPartition(df):
  '''
  Utility method to count & print the number of records in each partition
  '''
  print("Per-Partition Counts:")
  
  def countInPartition(iterator): 
    yield __builtin__.sum(1 for _ in iterator)
    
  results = (df.rdd                   # Convert to an RDD
    .mapPartitions(countInPartition)  # For each partition, count
    .collect()                        # Return the counts to the driver
  )

  for result in results: 
    print("* " + str(result))
   
# Apply function
printRecordsPerPartition(df)
