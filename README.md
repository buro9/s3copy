# s3copy
Copy the contents of an S3 bucket from one account to another

Please read the code... this is primitive and was written to solve a very specific purpose.

It is written in Go, it does work well :)

Assumptions based on the problem I was solving:
* All items in s3 were in the root of the bucket
* The only meta-data I cared about was the content-type
* I had the API keys for the original bucket, but not access to the account (there is a good story behind this... I'm an idiot)
* All things in the original account were only available privately, via credentials
* I wanted resume... so I only want to copy files not copied or where ETags differ

That rules out AWS provided sync tools, and even s3s3copy

What this looks like when being run:

```
./s3copy -from.bucket foobucket -from.key FOO_KEY -from.secret "FOO_SECRET" -to.bucket barbucket -to.key BAR_KEY -to.secret "BAR_SECRET"
Fetching from bucket index
Fetching to bucket index
198638 items in from bucket: foobucket
169 items in to bucket: barbucket
198487 items to be copied
4962 / 198487 [=>------------------------------------------------------------------] 2.50 % 2h49m40s
```

It's set to run 10 go routines concurrently, you can change this with a `-gophers 20` flag
