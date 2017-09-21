# Between-Class Questions #2

Michael Newton

# Problem 1
    A. Standard deviations of MAX Temps 
        Group One: 7.7098912
        Group Two: 10.7610954
        Group Three: 12.5660863
    I was able to do this by joining two Pair RDDs, mapping them to a Double RDD, and using sampleStdev to get the standard deviation.
    -----------------------------------------------
    B. Standard deviations of AVG Temps
        Group One: 7.5852841
        Group Two: 9.8743556
        Group Three: 11.3239132
    I was able to do this by joining two Pair RDDs, mapping them to find the average, joining them with the three groups, and then mapping them to a Double RDD, and using sampleStdev to get the standard deviation.
    -----------------------------------------------
    
C.
![Group One](https://raw.githubusercontent.com/CSCI3395-F17/daily-code-mnewtonten/master/images/groupOne.png?token=AVGCDUVS5SlsYCjNHLE50iL4iaXEnApWks5ZyfbswA%3D%3D)
![Group Two](https://raw.githubusercontent.com/CSCI3395-F17/daily-code-mnewtonten/master/images/groupTwo.png?token=AVGCDfHbZKte3PGywpQ0NG_0phzGbl0cks5ZyfgKwA%3D%3D)
![Group Three](https://raw.githubusercontent.com/CSCI3395-F17/daily-code-mnewtonten/master/images/groupThree.png?token=AVGCDdhqPTaqlNQyOH0GwF3iIoaNGmCkks5ZyfgOwA%3D%3D)

# Problem 2   

  Filter and map an RDD down to just (ID , VALUE) where value = max temp, and then use a group by key, ten join it with location of station and plot.

![Temperatures](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/question2.png?raw=true) 

# Problem 3 

a. Average for all of 2016 was 52.6863617 and average for all of 1897 was 50.4572105 Made a function that calculated average temp of the year. 
b. Average of both 2016 and 1897 is 51.5717861.



