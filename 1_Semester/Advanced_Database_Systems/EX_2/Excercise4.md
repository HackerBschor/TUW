Since creating the database from the files in '/home/adbs22/shared/hive/' was not possible, I copied the files to my hadoop home directory.

```bash
hadoop fs -mkdir hive_csv
hadoop fs -cp  /home/adbs22/shared/hive/* /user/e12132344/hive_csv
```