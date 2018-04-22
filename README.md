# Hadoop-sort_merge_hoin
This's is a program for join action in Hadoop using sort_merge method.<br>
The program is coding with Java.<br>

To run it, you need to rename the *.java file as the class name in the code.

###'join'<br>
run the program with command:<br>
java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/orders.tbl join:R0=S0 res:S1,R1,R5

It means join the R and S table by the join key where R0=S0,  and result columns are S1 R1 and R5.

output of 'same join keys':<br>
![image text](https://github.com/liuchengzimozigreat/Hadoop-sort_merge_join/blob/master/same_key_join_output.png?raw=true)<br>
