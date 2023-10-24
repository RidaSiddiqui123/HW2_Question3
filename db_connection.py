#-------------------------------------------------------------------------
# AUTHOR: Rida Siddiqui
# FILENAME: db_connection
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: 12 hrs
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
import string

def connectDataBase():
    # Create a database connection object using psycopg2
    # --> add your Python code here
    db_name = "Assignment 2 - web search"
    db_user = "postgres"
    db_pass = "3364"
    db_host = "localhost"
    db_port = "5432"

    try:
        conn = psycopg2.connect(database = db_name,
                                user = db_user,
                                password = db_pass,
                                host = db_host,
                                port=db_port)
        return conn
    except:
        print("Database not connected")

#this works
def createCategory(cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    sql = "Insert into category(id, name) Values(%s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)

#this works
def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    sql = "Select id from category where name = %(docCat)s"
    cur.execute(sql, {'docCat': docCat})
    recset = cur.fetchall()
    catId = int(recset[0][0])

    #docText = docText.translate(str.maketrans('', '', string.punctuation))

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    sql = "Insert into documents(docnumber, text, title, num_chars, date, category_id) Values(%s, %s, %s, %s, %s, %s)"
    recset = [docId, docText, docTitle, len(docText), docDate, catId]
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    #take individual terms from text, and seperate by spaces and remove puncuation
    #termsInDoc = docText.split()
    termsInDoc = docText.lower().translate(str.maketrans('', '', string.punctuation))
    termsInDocList = termsInDoc.split(' ')

    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
   # termsList = []

    for term in termsInDocList:
        sql = "Select terms.term from terms where terms.term = %(term)s"
        cur.execute(sql, {'term': term})
        recset = cur.fetchall()

        if recset == []:
            sql = "Insert into terms(term, num_chars) Values(%s, %s)"
            recset = [term, len(term)]
            cur.execute(sql, recset)
        else:
            pass
            #print(str(term) + " is already in table terms")

    termsInDocListWithCount = []
    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here

    unique_terms = list(set(termsInDocList))
    for term in unique_terms:
        count = termsInDocList.count(term)
        termsInDocListWithCount.append([term, count])
        sql = "Insert Into index(count, docnumber, term) Values(%s, %s, %s)"
        recset = [count, docId, term]
        cur.execute(sql, recset)

#this works
def deleteDocument(cur, docId):
    sql = "Select term from index where docnumber = %(docId)s"
    cur.execute(sql, {'docId': docId})
    recset = cur.fetchall()

    termsList = []
    for i, term in enumerate(recset):
        termsList.append(str(recset[i][0]))

    for term in termsList:
        # 1 Query the index based on the document to identify terms

        # 1.1 For each term identified, delete its occurrences in the index for that document
        sql = "Delete from index where docnumber = %(docId)s and term = %(term)s"
        #//this deletes rows from index where term is equal to speicifd term and docnumber is equal to 24
        #// this shoudl delete it
        cur.execute(sql, {'docId': docId, 'term': term})

        # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
        sql = "Select term from index where term = %(term)s"
        cur.execute(sql, {'term': term})
        recset = cur.fetchall()

        if recset:
            print("there is still a term in some other document. Cannot be deleted")

        else:
            sql = "Delete from terms where term = %(term)s"
            cur.execute(sql, {'term': term})

    # 2 Delete the document from the database
    sql = "Delete from documents where docNumber = %(docId)s"
    cur.execute(sql, {'docId': docId})

       # //i have to delete every occurence of the document in the table index before deleting it from the documents table

#this works
def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

#this works
def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here

    sql = "Select index.term, documents.title, index.count from index inner join documents on index.docnumber = documents.docnumber"
    cur.execute(sql)
    recset = cur.fetchall()


    for i, string in enumerate(recset):
        print(str(string[0]) + ":" + str(string[1]) + ":" + str(string[2]))



