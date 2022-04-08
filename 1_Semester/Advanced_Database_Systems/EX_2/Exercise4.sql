CREATE DATABASE e12132344 LOCATION "/user/e12132344/hive_db";
use e12132344;

CREATE TABLE badges
(
  `id` bigint ,
  `class` bigint ,
  `date` timestamp ,
  `name` string ,
  `tagbased` boolean ,
  `userid` bigint ) ROW FORMAT   DELIMITED
    FIELDS TERMINATED BY ','
    COLLECTION ITEMS TERMINATED BY '\002'
    MAP KEYS TERMINATED BY '\003'
  STORED AS TextFile TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA INPATH '/user/e12132344/hive_csv/badges.csv' INTO TABLE badges;

CREATE TABLE comments
(
  `id` bigint ,
  `creationdate` timestamp ,
  `postid` bigint ,
  `score` bigint ,
  `text` string ,
  `userdisplayname` string ,
  `userid` bigint )
ROW FORMAT   DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TextFile TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA INPATH '/user/e12132344/hive_csv/comments.csv' INTO TABLE comments;


CREATE TABLE postlinks
(
  `id` bigint ,
  `creationdate` timestamp ,
  `linktypeid` boolean ,
  `postid` bigint ,
  `relatedpostid` bigint )
ROW FORMAT   DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TextFile TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA INPATH '/user/e12132344/hive_csv/postlinks.csv' INTO TABLE postlinks;


CREATE TABLE posts
(
  `id` bigint ,
  `acceptedanswerid` bigint ,
  `answercount` bigint ,
  `body` string ,
  `closeddate` string ,
  `commentcount` bigint ,
  `communityowneddate` timestamp ,
  `creationdate` timestamp ,
  `favoritecount` bigint ,
  `lastactivitydate` timestamp ,
  `lasteditdate` timestamp ,
  `lasteditordisplayname` string ,
  `lasteditoruserid` bigint ,
  `ownerdisplayname` string ,
  `owneruserid` bigint ,
  `parentid` bigint ,
  `posttypeid` bigint ,
  `score` bigint ,
  `tags` string ,
  `title` string ,
  `viewcount` bigint )
ROW FORMAT   DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TextFile TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA INPATH '/user/e12132344/hive_csv/posts.csv' INTO TABLE posts;


CREATE TABLE users
(
  `id` bigint ,
  `aboutme` string ,
  `accountid` bigint ,
  `creationdate` timestamp ,
  `displayname` string ,
  `downvotes` bigint ,
  `lastaccessdate` timestamp ,
  `location` string ,
  `profileimageurl` string ,
  `reputation` bigint ,
  `upvotes` bigint ,
  `views` bigint ,
  `websiteurl` string )
ROW FORMAT   DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TextFile TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA INPATH '/user/e12132344/hive_csv/users.csv' INTO TABLE users;

CREATE TABLE votes (
  `id` bigint,
  `bountyamount` string,
  `creationdate` timestamp,
  `postid` bigint,
  `userid` string,
  `votetypeid` bigint
)
ROW FORMAT   DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TextFile TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA INPATH '/user/e12132344/hive_csv/votes.csv' INTO TABLE votes;

SELECT u.id, COUNT(*)
FROM users u
JOIN posts p ON (u.id = p.owneruserid)
LEFT SEMI JOIN (SELECT * FROM comments) c ON (c.postid = p.id)
LEFT SEMI JOIN (SELECT id, postid, count(*) FROM postlinks pl GROUP BY id, postid HAVING count(*) > 1) pl ON (pl.postid = p.id)
GROUP BY u.id
