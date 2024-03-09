# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE dev.dev_silver.stocks;
# MAGIC create table dev.dev_silver.stocks like dev.dev_bronze.stocks;
