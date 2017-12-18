# Final Project 2

The dataset I used for this final paper, found on kaggle.com is a collection of articles in their entirety called "All the News".

What I will be doing is examining the sentiment for the articles using sentiment analysis, and then using that as well as word and character count to see if it is possible to determine political bias.

Counts for publications
+--------------------+-----+

|         publication|count|

+--------------------+-----+

|           Breitbart|23776| + 2

|       New York Post|17493| + 1

|                 NPR|11992| - 1

|                 CNN|11488| - 1

|     Washington Post|11114| - 2

|             Reuters|10709| 0

|            Guardian| 8681| - 1

|      New York Times| 7803| - 2

|            Atlantic| 7179| - 1

|    Business Insider| 6756| 0

|     National Review| 6203| + 2

| Talking Points Memo| 5213| - 1

|                 Vox| 4928| - 1

|       Buzzfeed News| 4845| 0

|            Fox News| 4354| + 2

Using the following chart as well as mediabiasfactcheck.com and allsides.com I was able to come up with these bias ratings for the different websites, displayed to the right of the different publications.

The way I do my sentiment analysis is to make a list of all of the most used words in an article, and do sentiment analysis on the first 80 words that are most occuring. I also filter out the most common words, such as "the," "of," "to," "and," ect. 

I do this in a udf called "expand dataset," where I take in the DataFrame and the political bias label (1-5 based on publication). Then I expand it by reading number of characters, number of words, and then doing the filtered sentiment analysis.

For this small scale test I did, I limited the number of articles from each publication to 10, and ran my analysis. 

I was able to come up with these plots for each political bias.

![Very Conservative](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/veryconservative.png?raw=true)
![Conservative](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/conservative.png?raw=true)
![Neutral](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/neutral.png?raw=true)
![Liberal](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/liberal.png?raw=true)
![Very Liberal](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/veryliberal.png?raw=true)


I do my classifications based on word count, character count, and sentiment analysis using a random forest search.
My political bias analyis for this limited dataset came up being 31% accurate the first time I ran it, and 33.75% the second time 


To do analysis on 1,000 articles took my roughly 30 minutes, so to do my complete dataset (1000 of each publication) is going to take about 7.5 hours. I'll update this file with my findings tomorrow.

So after 10.6 hours of chugging along, my analysis is complete and I have these charts to show for it.

![VC](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/fp_vc.png?raw=true)
![C](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/fp_c.png?raw=true)
![N](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/fp_n.png?raw=true)
![L](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/fp_l.png?raw=true)
![VL](https://github.com/CSCI3395-F17/daily-code-mnewtonten/blob/master/images/fp_vl.png?raw=true)

From what I can tell from these plots, I was suprised to see that in both the cases of "liberal" vs "conservative" as well as "very liberal" vs "very conservative," the liberal side had a greater standard deviation of sentiment, though liberal articles were typically longer, and this might have something to do with it, it seemed like for the most part word count did not change the sentiment value very much, and many very long articles were quite neutral in their sentiment. 

The accuracy of my classification this time was 36.13%, which is a better score than I was getting before, and while it's not as good as I want, still better than just randomly guessing (as that would be 1/5 chance or 20%).

If I were to expand on this analysis, I would check to see if adding the number of words/characters in the title of an article helped refine my classification accuracy, and if I could classify the articles based on the "Getting Real About Fake News" dataset, on if they are more likely to be a bs or hate article.

