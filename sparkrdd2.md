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

    A. Average for all of 2016 was 52.6863617 and average for all of 1897 was 50.4572105 Made a function that calculated average temp of the year. 

    B. Average for all stations that were active in both 2016 and 1897 was 50.2034428 for 1897 and 53.2649334 for 2016.

C.

![All Stations](://raw.githubusercontent.com/CSCI3395-F17/daily-code-mnewtonten/master/images/3c.png?token=AVGCDWunITOZ7y-jkO4gJ7D7qZqyWlhUks5Z0d1hwA%3D%3D)


D.

![Both Year Stations](https://raw.githubusercontent.com/CSCI3395-F17/daily-code-mnewtonten/master/images/3d.png?token=AVGCDQ34Ck3C92ZfUZ176mutsRzuYq72ks5Z0d2lwA%3D%3D)


# Problem 4

    The merits of looking at all stations for all times is that hopefully, with more established stations you will have more accurate averages across the world. The problem with looking at all years this way is that it makes it an unfair comparison since you are looking at stations that didn't exist before.

# Problem 5

    One thing that made me skeptical was one point in the plot for my answers to 3b and 3c was very very low. I'm not sure how this one point differs from the rest of the data so much, but I think that it's possible that it could be an error. You could identify if this was a flaw or not by isolating the temperatures for that year and comparing them closley with another year, or analyzing the years directly before and after. you could also do it by looking at the data file itself to make sure all of the values are there. This would be hard to fix because if they were missing or corrupted it would not be possible to get the data.  


# InClass Questions

    1. Station USS0063P01S on 8/02/2017 with a difference of 212.58
    2. Station USS0045M07S with a difference of 231.3
    3. Standard Devietation of high Temps is 22.832941
      Standard Deviation of low temps is 20.9614385
    4. 1821 stations
