# Between-Class Questions #2

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
    
