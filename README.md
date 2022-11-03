# data_ingestion.py
the file contains a python program to read pdf files page by page, create meaningful snippets out of pages, give them unique ids, and save them into a mongoDB repository.
the program also has ability to remove the files, expire them by the dates, and have them back as active. the active snippets in the mongoDB repository will be used by a similarity search to appear as a query answers.
additionaly, the program has the ability to do updating, removing, etc simoultanously, so there is possibility to send many documents to be updated, removed, or expired.
the program was created for Oak Valley Health Human Resource Expert System (HRES). The project was done under Trinetra managementl.
