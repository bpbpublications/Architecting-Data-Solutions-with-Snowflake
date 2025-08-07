
/* get number of null present in key column*/
SELECT SNOWFLAKE.CORE.NULL_COUNT(
SELECT account_num
FROM retail_poc_db.poc.account_info
);

/* Define custom function */
CREATE OR REPLACE DATA METRIC FUNCTION retail_poc_db.poc.validate_account_number(
arg_t TABLE(
arg_acc_len NUMBER
 )
)
RETURNS STRING
AS
$$

SELECT
Case when Length(Account_number) = arg_acc_len then 'Valid Account' else 'Invalid Account' end as account_dq_indicator
FROM account_info;

$$;


/* LLM Functions */

/* remove access to cortex user role */\
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE PUBLIC;


/* create custom role and assign cortex role to the custom role */
USE ROLE ACCOUNTADMIN;
CREATE ROLE custom_cortex_role;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE custom_cortex_role;

/*grant access to user */
GRANT ROLE custom_cortex_role TO USER poojakelgaonkar;

/* COMPLETE LLM Function */\

/* run individual prompt using COMPLETE */
SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', 'What are the use cases to implement large language models?');

/* run on table column*/
SELECT SNOWFLAKE.CORTEX.COMPLETE(
 'mistral-large',
 CONCAT('Rate the movie review: <review>', content, '</review>')
) FROM movie_reviews LIMIT 10;

/* SENTIMENT */

/* get sentiment score */
SELECT SNOWFLAKE.CORTEX.SENTIMENT(review_comment), review_comment FROM movie_reviews LIMIT 10;\

/* SUMMARIZE */

/* generate summary of news column*/
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(news_content) FROM daily_news LIMIT 10;\

/* TRANSLATE */

/* translate input prompt*/
SELECT SNOWFLAKE.CORTEX.TRANSLATE(news_content, 'en', 'fr') FROM daily_news LIMIT 10;\

/* EXTRACT ANSWER */

/* generate answer from input */
SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(movie_review, 'What actors does this movie review mention?')
FROM movie_reviews LIMIT 10;


/* create database for poc */
CREATE DATABASE LLM_POC;
CREATE SCHEMA POC;
USE DATABASE LLM_POC;
USE SCHEMA POC;


/* create movie reviews table */
CREATE TABLE MOVIE_REVIEWS
(
MOVIE_NM STRING, 
MOVIE_REVIEWS STRING, 
MOVIE_RATINGS float
);

/* create news table */
CREATE TABLE NEWS_TBL
(
NEWS_NM STRING,
NEWS STRING,
NEWS_DT DATE,
NEWS_COMMENT STRING
);


 /* load sample row into movie reviews */
INSERT INTO MOVIE_REVIEWS VALUES('Arthur The King','Michael Light (Mark Wahlberg) has one last chance to win a championship and prove himself. Finding sponsorship and creating a team of four is challenging, given his washed-out career and a previous disastrous race. However, he assembles a team of an athlete and a social media star, Leo (Simu Liu), free climber Olivia (Nathalie Emmanuel), and Chik (Ali Sulaiman), who has a bad knee. During the race, he has a fifth and an unlikely teammate an injured indie dog, Arthur.Director Simon Cellan Jones\'92s offering is an account of companionship between man and dog. The story talks about loyalty, friendship, sacrifice, and survival. It also offers ample adventure through a jungle trek, rock climbing with bicycles, ziplining across a valley (a sequence that will have you on the edge of your seat), night runs, and more. Cinematographer Jacques Jouffret splendidly captures the Dominican terrains, forests, mountains, rivers, and valleys. The adventurous race also looks convincing and authentic. Writers Michael Brandt and Mikael Lindnords narrative has all the ingredients you expect from this genre\'97friction amid friendship, obstacles, and the tough decisions the athletes must make. Although the story gets a tad too sappy, the narratives even pacing prevents it from weighing you down. Mark Wahlberg delivers a powerful performance as the out-of-luck captain who wants to make a comeback no matter what. Nathalie Emmanuel, Simu Liu, and Ali Suliman are also worthy additions. Their banter and individual reasons for participating in the race add authenticity to the narrative. The movie real star, however, is the dog. It\'92s inspiring and incredulous to think this is a true story (of Michael Lindnord), and the narrative will tug at your heartstrings. The movie does not end with the race or whether the Broadrail team wins and goes beyond being an adventure sports drama. Whether you are an animal lover or an adventure sports enthusiast, Arthur the King will thrill you and warm your heart. Be warned: you may tear up at this endearing story of a dog and underdogs!',3.5);

/* load news table */
INSERT INTO NEWS_TBL VALUES ('NEWYORK TIMES' , 'Researchers first discovered the link between heat and aggression by looking at\'a0crime data, finding that there are more murders, assaults and episodes of domestic violence on hot days. The connection applies to nonviolent acts, too: When temperatures rise, people are more likely to engage in hate speech and honk horn in traffic. Lab studies back this up. In one experiment in 2019, people acted more spitefully toward others while playing a specially designed video game in a hot room than in a cool one.So-called reactive aggression tends to be especially sensitive to heat, most likely because people tend to interpret others actions as more hostile on hot days, prompting them to respond in kind. Kimberly Meidenbauer, an assistant professor of psychology at Washington State University, thinks this increase in reactive aggression may be related to heats effect on cognition, particularly the dip in self control. Your tendency to act without thinking, or not be able to stop yourself from acting a certain way, these things also appear to be affected by heat, she said.' , current_date, 'How Heat Affects the Brain. High temperatures can make us miserable. Research shows they also make us aggressive, impulsive and dull.');



/* Using extract answer function */
SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(MOVIE_REVIEWS,
 'What movie does the review mention?')
FROM MOVIE_REVIEWS;

/* Using sentiment function */
SELECT SNOWFLAKE.CORTEX.SENTIMENT(MOVIE_REVIEWS), MOVIE_NAME FROM MOVIE_REVIEWS;

/* using summarize function */
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(MOVIE_REVIEWS) FROM MOVIE_REVIEWS;

/* using translate function */
SELECT SNOWFLAKE.CORTEX.TRANSLATE(MOVIE_REVIEWS, 'en', 'fr') FROM MOVIE_REVIEWS;


CREATE OR REPLACE CORTEX SEARCH SERVICE ticket_search_service_dev
ON transcript_text
ATTRIBUTES region
WAREHOUSE = dev_cortex_search_wh
TARGET_LAG = '1 day'
EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
SELECT
ticket_desc,
Ticekt_id,
Open_date, 
Status, 
agent_id
 FROM support_tickets
);

/* USAGE */

/* Metadata query to get document ai service compute*/
SELECT 
ACCOUNT_NAME,
SUM(CREDITS_USED) AS TOTAL_CREDITS_USED,
SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS,
SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS
FROM SNOWFLAKE.ORGANIZATION_USAGE.METERING_DAILY_HISTORY
WHERE service_type ILIKE '%ai_services%' AND
start_time <= current_time
;
