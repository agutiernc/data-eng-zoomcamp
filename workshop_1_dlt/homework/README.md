# Homework

The [linked colab notebook](https://colab.research.google.com/drive/1Te-AT0lfh0GpChg1Rbd0ByEKOHYtWXfm#scrollTo=wLF4iXf-NR7t&forceEdit=true&sandboxMode=true) offers a few exercises to practice what you learned today.


#### Question 1: What is the sum of the outputs of the generator for limit = 5?
**Answer**
`C: 8.382332347441762`

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)
sum_of_outputs = sum(generator)
print(sum_of_outputs)

for sqrt_value in generator:
    print(sqrt_value)
```
#### Question 2: What is the 13th number yielded by the generator?
**Answer**
`B: 3.605551275463989`

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

for sqrt_value in generator:
    print(sqrt_value)
```

#### Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
**Answer**

`A. 353`

```python
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

def append_people():
    for person in people_1():
        yield person
    for person in people_2():
        yield person

# Calculate the sum of all ages
total_age = sum(person['Age'] for person in append_people())

print(total_age)
```

#### Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.

**Answer**

`B. 266`

```python
import dlt
import duckdb

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# define the connection to load to.
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='people')

# load generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
                               table_name="people_2",
                               write_disposition="merge",
                               primary_key="ID")

# load the next generator to the same or to a different table.
info = generators_pipeline.run(people_2(),
                               table_name="people_2",
                               write_disposition="merge",
                               primary_key="ID")

# connect to duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# show tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data
print("\n\n\n people table below:")

peeps = conn.sql("SELECT * FROM people_2").df()
display(peeps)

# Calculate the sum of ages of all the people loaded as described above
age_sum = conn.sql("SELECT SUM(age) FROM people_2").df()
display(age_sum)
```