# s3copy
Copy the contents of an S3 bucket from one account to another

Please read the code... this is primitive and was written to solve a very specific purpose.

It is written in Go, it does work well :)

Assumptions based on the problem I was solving:
* All items in s3 were in the root of the bucket
* The only meta-data I cared about was the content-type
* I had the API keys for the original bucket, but not access to the account (there is a good story behind this... I'm an idiot)
* All things in the original account were only available privately, via credentials

That rules out AWS provided sync tools, and even s3s3copy
