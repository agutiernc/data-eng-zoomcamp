import sys
import pandas as pd 

# print arguments
print(sys.argv)

# argument 0 is the name of the file
# argumment 1 contains the actual first argument we care about
day = sys.argv[1]

# some fancy stuff with pandas

print (f'job finished successfully for day = {day}')


# docker run -it \
#   -e POSTGRES_USER="root" \
#   -e POSTGRES_PASSWORD="root" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
#   -p 5432:5432 \
# postgres:15