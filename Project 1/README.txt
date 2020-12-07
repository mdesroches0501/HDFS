---------------------------------------------------------IMPORTANT------------------------------------------------
| Everything provided in canvas turn in is just the .java files. Outputs of the project are located in 
| Project1 folder on the cs433 server provided by UNR. The generated .txt files are per the instructions here
| and have already been tested using different names for the .txt files on the server. These Instructions are for 
| the a system that has the proper instalation of Hadoop. 
|---------------------------------------------------------TO RUN Q1------------------------------------------------
| Access Q1 Directory |
| hadoop com.sun.tools.javac.Main HashtagCount.java
| jar cf cc.jar HashtagCount*.class
| hadoop jar cc.jar HashtagCount /homework1/tweets.txt /homework1/users.txt /user/mdesroches/popular_hashtag.txt
|---------------------------------------------------------TO RUN Q2------------------------------------------------
| Access Q2 Directory |
| hadoop com.sun.tools.javac.Main CitiesCount.java
| jar cf cc.jar CitiesCount*.class
| hadoop jar cc.jar CitiesCount /homework1/tweets.txt /homework1/users.txt /user/mdesroches/most_tweeted_cities.txt
|-------------------------------------------------------------------------------------------------------------------