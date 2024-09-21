#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo


# In[4]:


client = pymongo.MongoClient("mongodb://localhost:27017/")


# In[5]:


mydb = client["Assignment_1"]
collection = mydb.Students


# ## Assignment 1
# **Deadline**: 23rd September 2024
# 
# This assessment consists of 20 MongoDB queries, ranging from easy to hard. You are required to use the following sample data:
# 
# ```json
# {
#     "students": [
#         {"name": "Rohit", "age": 23, "math_score": 85, "physics_score": 90, "city": "New York"},
#         {"name": "Eram", "age": 22, "math_score": 78, "physics_score": 75, "city": "Los Angeles"},
#         {"name": "Madan", "age": 24, "math_score": 95, "physics_score": 88, "city": "Chicago"},
#         {"name": "Uvaish", "age": 21, "math_score": 60, "physics_score": 65, "city": "Houston"},
#         {"name": "Neha", "age": 23, "math_score": 72, "physics_score": 80, "city": "Phoenix"}
#     ]
# }
# 
# ```
# 
# Please submit your solutions by the given deadline.
# 

# 1. Insert the given sample data into a MongoDB collection called 'students'.

# In[12]:


Student_1 = {"name": "Rohit", "age": 23, "math_score": 85, "physics_score": 90, "city": "New York"}
Student_2 = {"name": "Eram", "age": 22, "math_score": 78, "physics_score": 75, "city": "Los Angeles"}
Student_3 = {"name": "Madan", "age": 24, "math_score": 95, "physics_score": 88, "city": "Chicago"}
Student_4 = {"name": "Uvaish", "age": 21, "math_score": 60, "physics_score": 65, "city": "Houston"}
Student_5 = {"name": "Neha", "age": 23, "math_score": 72, "physics_score": 80, "city": "Phoenix"}


collection.insert_many([Student_1,Student_2, Student_3,Student_4,Student_5])


# 2. Write a query to find all students who have a 'math_score' greater than 80.

# In[13]:


query = ({"math_score": {"$gt":80}})

for student in collection.find(query):
    print(student)


# 3. Write a query to find students whose 'age' is less than 23.

# In[14]:


query = ({"age": {"$lt":23}})

for student in collection.find(query):
    print(student)


# 4. Write a query to return only the 'name' and 'math_score' of all students.

# In[15]:


for student in collection.find({}, {"_id": 0, "name": 1, "math_score":1}):
    print(student)


# 5. Write a query to find students from the city 'New York'.

# In[17]:


query = ({"city": "New York"})

for student in collection.find(query):
    print(student)


# 6. Write a query to update the 'physics_score' of 'Bob' to 85.

# In[18]:


collection.update_one({"name": "Neha"}, {"$set": {"physics_score": 85}})


# 7. Write a query to delete the student 'David' from the collection.

# In[ ]:


collection.delete_one({"name": "Uvaish"})


# 8. Write a query to find all students where 'math_score' is between 70 and 90 (inclusive).

# In[19]:


query = ({"math_score": {"$gte":70, "$lte":90}})

for student in collection.find(query):
    print(student)


# 9. Write a query to find students whose 'math_score' is greater than 'physics_score'.

# In[27]:


query = {"$expr": {"$gt": ["$math_score", "$physics_score"]}}

for student in collection.find(query):
    print(student)


# 10. Write a query to return students sorted by 'math_score' in descending order.

# In[31]:


sorted_students = collection.find().sort("math_score", -1)
for student in sorted_students:
    print(student)


# 11. Write a query using the aggregation framework to calculate the average 'math_score' of all students.

# In[38]:


pipeline = [
    { "$group": {
            "_id": None,
            "average_math_score": {"$avg": "$math_score"}}}]

result = collection.aggregate(pipeline)

for score in result:
        print(f"Average Math Score: {score['average_math_score']}")


# 12. Write a query to group students by 'city' and count the number of students in each city.

# In[43]:


pipeline = [
    {"$group": {"_id": "$city", "count": {"$sum": 1}}}
]
city_groups = collection.aggregate(pipeline)
for group in city_groups:
    print(group)


# 13. Write a query to find students who either have 'math_score' greater than 80 or 'physics_score' greater than 85.

# In[44]:


query = {
    "$or": [
        {"math_score": {"$gt": 80}},
        {"physics_score": {"$gt": 85}}
    ]
}

for student in collection.find(query):
    print(student)


# 14. Write a query to find students whose age is exactly 23 and live in 'Phoenix'.

# In[47]:


query = {
    "$and": [
        {"age": 23,},
        {"city":"Phoenix"}
    ]
}

for student in collection.find(query):
    print(student)


# 15. Write a query to find students whose 'math_score' is not equal to 85.

# In[48]:


query = {"math_score": {"$ne": 85}}
for student in collection.find(query):
    print(student)


# 16. Write a query to find students whose 'name' starts with the letter 'A'.

# In[55]:


query = {"name": {"$regex": "^A", "$options": "i"}}
for student in collection.find(query):
    print(student)


# 17. Write a query to find students whose 'city' is neither 'New York' nor 'Los Angeles'.

# In[56]:


query = {"city": {"$nin": ["New York", "Los Angeles"]}}
for student in collection.find(query):
    print(student)


# 18. Write a query to update all students with 'age' greater than 22 to add a new field 'graduated': true.

# In[62]:


collection.update_many({"age":{"$gt":22}}, {"$set": {"graduated":True}})


# 19. Write a query to remove the 'physics_score' field for students who live in 'Chicago'.

# In[63]:


collection.update_many({"city": "Chicago"}, {"$unset": {"physics_score": ""}})


# 20. Write a query to find the student with the highest 'math_score'.

# In[65]:


sorted_students = collection.find().sort("math_score", -1).limit(1)
for student in sorted_students:
    print(student)


# In[ ]:




