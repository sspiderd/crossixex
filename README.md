This is the exercise as requested by the crossix team, it is written in Scala.

The main (and only) class that runs this is located at crossix.ex.CSVSorter
Tests are located at crossix.ex.ExTests

Make sure that scala is installed on the testing machine.
Bash is required to run the program from command line.

In order to run the exercise use the following command:
<code>run.sh <input file\> <output file\> <header to sort by\> <available memory\> <parallelism\> </code>

All arguments are mandatory

A sample csv file has been provided for ease of testing
To test it you can use the following:

<code>run.sh sample.csv /tmp/out.csv header1 3 2</code>

Feel free to provide your own csv file

As for the given questions:
1. The algorithm initially takes the input file, reads chunks of size 'm', sorts them
and outputs to an initial temporary folder. Note that Timsort (the default scala sorter) 
requires O(m) space so we can actually hold only m/2 records in memory. however, for 
simplicity's sake let's assume that we actually have 2m space and use only m for sorting. Headers
are always included (also for the next steps) so we can sort by them and also add them
to the final output.
* When parallelism > 1 each process is given a consecutive id number (0..p) and each process
then reads only lines which line number is process_id % parallelism. (That is, each process
is in charge of n/p lines. This method is used since the total number of lines is unknown
initially).

The algorithm then 'merge-sorts' every 2 files and outputs the result to a file in a new
directory, the files are read line by line and the lower valued line is then immediately
written out to disk so that there are never more than 2 rows in memory
* When parallelism > 1 every process will only read the files whose ordinality is 
(process_id + 1) % parallelism. Process id '0' will always read the first 2 files in
order to avoid some edge cases.

This continue until only 1 file is left

2. Complexity analysis for a single process:    
The initial sorting needs to sort m rows n/m times therefore the complexity for the first
step is O(m*log(m)*n/m) => O(n*log(m))

Every step thereafter requires reading each row once and outputting it to a new file. Initially we have n/m files
and each step merges 2 of them into 1 so the total number of steps is log(n/m).

Concluding: The first step is O(n*log(m)), the second step is O(n*log(n/m)) => O(n*log(m)+n*log(n/m)) =>
O(nlog(m)+nlog(n)-nlog(m)) => O(n*log(n))

* When parallelism > 1 each process is given n/p rows initially, the sorting of takes m*log(m) time:
O(m*log(m)*n/p)
Every step thereafter each process is given n/p rows to deal with, in each step we merge 2 files where initially a process
is given n/(m*p) files so the total number of steps is O(log(n/mp)) 
At some point we are left with p files so we can no longer use all of our processes to merge them. 
the fastest resolution will be to give half the processes 2 files each, 
then a quarter of the processes will be given 2 files each, etc. until we are left with a single file.
The total number of steps is log(p)
of these processes the longest running process will be the one that will do the final merge. That process will
run n operations on it's final step, n/2 in the step before that until log(p) iterations are made. so:
(n+n/2+n/4+...) {logp times}. This is a sum of geometric series => O(2*n - 2*n/p) => O(n(1-1/p))

Concluding: The first step is O(n*log(m)/p), the second step is O(n/p*log(n/mp)), the third step is  O(n)=>
O(n*log(m)/p + n/p(log(n)-log(m)-log(p))) + n) => O(n*log(m)/p + n*log(n)/p - n*log(m)/p - n*log(p)/p + n - n/p)
=> O(n+ n*log(n)/p - n*log(p)/p - n/p) => O(n+n*log(n)/p)

