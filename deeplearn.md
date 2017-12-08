# In Class Deeplearn

# Between Class Deeplearn

a. I first tried to reduce the amount of data used for training to 20%, and that lowered the precision from 23% to 19%, but increased the accuracy from 11% to 18%. 50% increased the accuracy to 24% and the precision to 33%, becoming the best precision so far. After I increased the iterations from 1000 to 3000 the accuraccy dropped slightly to 22% but the precision increased to 50%. After looking at the deeplearning4j documentation I learned that the sigmoid activation function is similar to the tanh activation function being used, so after switching them I got slightly better overall results with precision at 38.6% and accuracy at 42.1%

b. 

After using a similar setup with the next file, I was able to achive an accuracy of 78.6% accuracy and 67.4% accuracy with the tanh activation function and 74.6% accuracy and 64.7% precision with the sigmoid function, showing it did not help as much in this case.
