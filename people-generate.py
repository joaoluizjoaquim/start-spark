#pip install pymongo
#pip install Faker

import pymongo
from faker import Faker

fake = Faker('pt_BR')
cli = pymongo.MongoClient('mongodb://localhost:30401')
db = cli['acme']
for count in range(0,10000):
	doc = {
		'_id': count,
		'nome': fake.name(),
		'email': fake.email(),
		'data_nascimento': fake.date_time_between(start_date="-30y",
		end_date="now", tzinfo=None),
		'endereco': fake.street_name(),
		'numero': fake.building_number(),
		'bairro': fake.bairro(),
		'cidade': fake.city(),
		'estado': fake.estado_sigla(),
		'cep': fake.postcode(),
		'telefone': fake.phone_number(),
	}
	db.funcionarios.insert_one(doc)
	print(doc['nome'])
cli.close()
