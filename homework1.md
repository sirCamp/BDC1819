### Assignment

After doing the preliminary set up of your machine as explained above, create a program GxxHM1.java (for Java users) or GxxHM1.py (for Python users), where xx is your two-digit group number, which does the following things:

- Reads an input file (dataset.txt) of nonnegative doubles into an RDD dNumbers, as in the template.
- Computes and prints the maximum value in dNumbers. This must be done in two ways:
  + using the reduce method of the RDD interface;
  + using the max method of the RDD interface 
  (For Java users, read here about some work-around require to pass a comparator to the method.)
- Creates a new RDD dNormalized containing the values of dNumbers normalized in [0,1].
- Computes and prints a statistics of your choice on dNormalized. Make sure that you use at least one new method provided by the RDD interface.
- Add short but explicative comments to your code and when you print a value print also a short description of what that value is.
- Return the file with your program (GxxHM1.java or GxxHM1.py) by mail to bdc-course@dei.unipd.it
