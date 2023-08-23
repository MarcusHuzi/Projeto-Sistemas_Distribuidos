import rpyc
import sys
import os
import logging
import re

logging.basicConfig(level=logging.DEBUG)


def get(master, file):
	file_table = master.read(file)
	if not file_table:
		logging.info("Arquivo nao encontrado")
		return

	for block in file_table:
		for host, port in block['block_addr']:
			try:
				con = rpyc.connect(host, port=port).root
				data = con.get(block['block_id'])
				if data:
					sys.stdout.write(data)
					break
			except Exception as e:
				continue
		else:
			logging.error("Erro ao procurar blocos do arquivo")

def delete(master, file):
	file_table = master.delete(file)
	if not file_table:
		logging.info("Arquivo nao encontrado")
		return
	for block in file_table:
		for host, port in block['block_addr']:
			try:
				con = rpyc.connect(host, port=port).root
				con.delete(block['block_id'])
			except Exception as e:
				continue
	print('arquivo deletado/n')

def put(master, source, dest):
	size = os.path.getsize(source)
	blocks = master.write(dest, size)
	if(blocks == -1):
		print("Erro na gravação do arquivo")
	else:
		with open(source) as f:
			for block in blocks:
				data = f.read(master.block_size)
				block_id = block['block_id']
				minions = block['block_addr']

				minion = minions[0]
				minions = minions[1:]
				host, port = minion

				con = rpyc.connect(host, port=port)
				con.root.put(block_id, data, minions)


def authenticate():
	
	# get usernames
	fh = open('users.conf', mode='r', encoding='cp1252')
	users=re.findall(r'Username: .*', fh.read())
	usernames=list()
	for i in range(0, len(users)):
		usernames.append(str(users[i]).split()[1])
	fh.close()
	fh = open('users.conf', mode='r', encoding='cp1252')
	passes=re.findall(r'Password: .*', fh.read())
	passwords=list()
	for i in range(0, len(passes)):
		passwords.append(str(passes[i]).split()[1])
	fh.close()

	# dict with usernames:passwords 
	auth_dict = {}
	for i in range(0, len(users)):
		entry={usernames[i]:passwords[i]}
		auth_dict.update(entry)
	
	# authenticate username
	# initialize an auth status 
	auth_status = ''
	
	# give user 3 attempts
	for i in range(0, 3):
		if auth_status == 'Valid username.':
			# go to password auth 
			pass
		
		else:
			# get username 
			username = input('username: ')
		
			# initialize username auth 
			username_auth = []
			ct = 0
			for key, value in auth_dict.items():
				ct += 1
				if username == key:
					# get specific username index in dictionary 
					username_auth.append(ct)
				else:
					username_auth.append(0)
							
			if i <= 1:
				if sum(username_auth) > 0:
					auth_status = 'Valid username.'
					continue
				else:
					print('Usuario nao existe. Voce tem ' +str(2-i) + ' tentativas sobrando.')
					continue 			
			else:
				if sum(username_auth) > 0:
					auth_status = 'Valid username.'
					continue
				else:
					print('Usuario nao existe. Voce nao tem mais tentativas.\nSaindo agora....')
					sys.exit()
	
	# authenticate password 
	# get the index of the user in the auth_dict to check password in that index
	user_index = sum(username_auth)

	# re-initialize auth status
	auth_status = ''
	for i in range(0, 3):
		if auth_status == 'Valid password.':
			# pass authentication 
			pass
			
		else:
			# get password
			password = input('password: ')
			
			# initialize password auth 			
			password_auth = []
			ct = 0
			for key, value in auth_dict.items():
				ct += 1
				if password == value:
					password_auth.append(ct)
				else:
					password_auth.append(0)
			
			if i <= 1:
				if sum(password_auth) > 0:
					# check that index of password matches user index
					if user_index == sum(password_auth):
						auth_status = 'Valid password.'
						continue
					else:
						print('Senha errada. Voce tem ' +str(2-i) + ' tentativas sobrando.')
						continue
				else:
					print('Senha errada. Voce tem ' +str(2-i) + ' tentativas sobrando.')
					continue
			else:
				if user_index == sum(password_auth):
					auth_status = 'Valid password.'
					continue
				else:
					print('Senha errada.Voce nao tem mais tentativas.\nSaindo agora....')
					sys.exit()
					
	# Final auth after passing all checks
	print('Autorização concedida.')					
	final_authorization = (username, password)
	return final_authorization

def main():

	nome, _ = authenticate()
		
	con = rpyc.connect("localhost", port=2131)
	master = con.root

	command = input('Selecione o comando[get, put, delete, exit]: ')

	if command == "get":
		f = input('Informe o arquivo: ')
		f = nome + f
		get(master, f)
		print("\n")
	elif command == "delete":
		f = input('Informe o arquivo: ')
		f = nome + f
		delete(master, f)
	elif command == "put":
		f = input('Informe o arquivo: ')
		n = input('Informe o nome do arquivo: ')
		n = nome + n
		put(master, f, n)
	elif command == "exit":
		sys.exit()
	else:
		logging.error("Comando invalido")


if __name__ == "__main__":
	main()
