# Databricks notebook source
# From doc: https://docs.databricks.com/en/dbfs/mounts.html#id1

aws_bucket_name = "dev-landing-layer"
mount_name = "dev-landing"
dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))
