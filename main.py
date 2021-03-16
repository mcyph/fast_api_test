#/usr/bin/python3
"""
Create a simple REST API service in Spring Boot, store the data in JSON
format in MongoDB and perform some simple data validations:

* Store the JSON request in MongoDB (refer to below incoming request sample using "id":"{{$randomInt}}") into:

* An “incoming” collection
    * Validate the data types of the incoming JSON request. (string, boolean, integer)
    * Build the following methods to manage the data and store the results in a MongoDB
      “outgoing” collection:oSelect the largest number from the array “numbersMeetNumbers”
* Find any duplicates in the string “findDuplicates”
* Remove whitespaces from “whiteSpacesGalore” without using replace()
* Store the results of the above methods in a MongoDB “outgoing” collection for a GET request
* Write a unit test for one of the above methods (positive and negative)

Incoming Request Sample:

curl --location --request POST http://localhost:8080/incoming --header "Content-Type: application/json" --data-raw "{ \"id\":\"652\", \"findDuplicates\":\"HereWeHaveSomeDuplicatedCharacters\", \"whiteSpacesGalore\":\"Can we replace all this white spaces without using replace please\", \"validateMeOnlyIActuallyShouldBeABoolean\":false,\"numbersMeetNumbers\":[35, 2, 100, 15, 75, 25, 99]}"
"""

import string
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient


client = MongoClient(port=27017)
db = client.simple_rest


app = FastAPI()


class Req(BaseModel):
    id: str
    findDuplicates: str
    whiteSpacesGalore: str
    validateMeOnlyIActuallyShouldBeABoolean: bool
    numbersMeetNumbers: List[int]


@app.get("/")
@app.get("/outgoing")
async def outgoing():
    """
    View any previously queued requests,
    clearing the "incoming" mongodb queue

    :return: a JSON object with the processed values
    """
    r = list(db.simple_rest_table.find())

    for i in r:
        # ObjectId's aren't encodable by fastapi!
        del i["_id"]

    db.simple_rest_table.remove()
    return r


@app.post("/incoming")
async def incoming(req: Req):
    """
    Do processing on the incoming POST data as per the problem
    detailed at the top of this file.

    :param req: An instance of `Req`, the parameters provided
                as the body of the POST request as JSON.
    :return: a dict with the processed output after inserting
             it into the MongoDB database.
    """

    # Because the original example was a decimal in a string,
    # assume that alphabetic and floating point values are
    # not permissible
    my_id = req.id
    assert my_id.isdecimal()

    # Record the positions of characters which are duplicates as
    # {character: [position 1, position 2, ...]}
    duplicates = {}
    used_previously = set()
    for x, c in enumerate(req.findDuplicates):
        if c in used_previously:
            duplicates.setdefault(c, []).append(x)
        else:
            used_previously.add(c)

    # Remove all whitespace characters from `whiteSpacesGalore`
    whiteSpacesGalore = ''.join(
        i for i in req.whiteSpacesGalore if i not in string.whitespace
    )

    # Validation should already have been performed
    # for both of these (boolean and a list of ints)
    validateMeOnlyIActuallyShouldBeABoolean = bool(req.validateMeOnlyIActuallyShouldBeABoolean)
    numbersMeetNumbers = [int(i) for i in req.numbersMeetNumbers]

    out = {
        "originalId": my_id,
        "duplicatePositions": duplicates,
        "whiteSpacesRemoved": whiteSpacesGalore,
        "onlyBoolean": validateMeOnlyIActuallyShouldBeABoolean,
        "listOfIntegers": numbersMeetNumbers
    }
    db.simple_rest_table.insert_one(out.copy())
    return out


if __name__ == '__main__':
    from fastapi.testclient import TestClient

    client = TestClient(app)

    def test_read_main():
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"msg": "Hello World"}
