# SpamReviewDetection


### Project Goal

It is a research project that we (me and another student at the lab) worked together to design and implement a spam reviews detection model based on **Hadoop and HBase**.

The data comes from the dianping.com, china's biggest crowd-sourced reviews platform about local businesses, like the Yelp in the US. The data had already been labeled so we built a supervised machine learning model. Based on the raw review, we created several features that may reflect the true credibility of merchants and customers. Then, combined with the language features such as part-of-speech tagging, we used both batch and streaming learning methods. 

### Code

I was mainly responsible for the following parts of this project:

1. Preprocess (com.brofan.service.preprocessor)
2. User features (com.brofan.service.feature.user)
3. Classfier (com.brofan.service.classfier)
4. Training Process (com.brofan.service.trainer)

### Explanation 

1. Object oriented design to better handle missing or malformed data, easier to read. Write outputs to multiple HBase tables.
2. Calculate user features by writing MapReduce program.
3. Use Mahout to build machine learning models. Transfer data between HDFS and Local file system. 
4. Link the feature engineering and machine learning process. 



