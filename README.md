#Cascading example
## Introduce the job to sovle
    File's style is CSV, and content is like `cookie,date`,for example `1001,130420`.Now we accomplish the task as below:
    * Static everyday's cookie number;
    * static everyday's different cookie number;

##Solution in detail:
### import cookielog and split by ",", then map with Fields
Cascading supply a easy way to deal with this problem by using `TEXTDelimited` called scheme:
    Tap source = new HFS(new TextDelimited(inputField, ","), inputpath)
###Static Everyday's count of same cookie,use:
* `GroupBy` groupby "date and cookie"
* `every` count the number of same date and cookie-`date_cookie_count`

###Static distinct count and total
* `sumBy` calculate the sum of `date_cookie_count`-`count`
* `countBy` calulate the count of cookie in same date -`distinct_count`
* `AggregteBy` groupBy `date` and caluate the sum and count at the same time

##Use
###compile

    $ mvn install

###run

    $ hadoop -jar target/wordCountIncascading-*.jar data/cookie.csv output

and then you will see result file in output dir, have fun
