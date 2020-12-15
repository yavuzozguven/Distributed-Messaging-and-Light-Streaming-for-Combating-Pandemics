
# Distributed-Messaging-and-Light-Streaming-for-Combating-Pandemics

The study consists of 3 tasks.These tasks are performed using the following dataset.
Rabindra Lamsal,CORONAVIRUS (COVID-19) GEO-TAGGED TWEETS DATASET,accessed 02 December 2020,https://ieee-dataport.org/open-access/coronavirus-covid-19-geo-tagged-tweets-dataset

## Translation of tweets matching with user-defined bounding boxes
There are latitude and longitude values of each tweet in the dataset. The user sends the tweet and the latitude and longitude values of this tweet to the function as well. This tweet, which comes into function, is translated into Turkish with the Google Cloud Translation API. According to the latitude and longitude values of the tweet coming to the function, it is published in the topic defined in the function (latitude-longitude values of the specified countries). However, the consumer can read messages from all topics.

## Recognition name entities in tweets
Name entity recognition is applied to the tweets coming to the function with a pre-trained model. The resulting message is divided into 3 parts. 
The first part is the word itself. The second part is about tags which this word indicates. The third part is a numerical data which is about how correct the tag of the word is. These data are published on the specified topic. Since the predict phase may take a long time, working with high specs machines in this section may give faster results.

## Performing skyline queries on tweets
The dataset includes the favorite count of each tweet and the count of followers of the person who posted this tweet. The user sends this data to the function. The function writes this data to a txt file. In the function, this txt file is read with the command to be sent by the user and assigned to a variable. Then, a skyline query is executed so that the count of favorites is minimum and the count of followers is maximum. The result is published on the specified topic.



# Citation

In prep. for Journal of Ambient Intelligence and Humanized Computing:

    @ARTICLE{8768370,
    author={Y.M. {Özgüven} and S. {Eken}},
    journal={Journal of Ambient Intelligence and Humanized Computing},
    title={Distributed Messaging and Light Streaming System for Combating Pandemics: A Case Study on Spatial Analysis of COVID-19 Geo-tagged Twitter Dataset},
    year={2020},
    month={},
    volume={},
    number={},
    pages={},
    keywords={Topic-based publish-subscribe, name entity recognition, spatial analysis, skyline query, geo-tagged twitter data, translation, Apache Pulsar},
    doi={},
    ISSN={},}