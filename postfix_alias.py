#!/usr/bin/env python

import sys
import MySQLdb
from optparse import OptionParser

DBNAME = "postfix"
DBUSER = "aliaser"
f = open("postfix_alias.key", "r")
DBPASSWD = f.readline().rstrip()
f.close()


cache = {}
tree = {}


def expand_email(e):
	if e.find("@") == -1:
		return e+"@jongedemocraten.nl"
	return e

def get_tree(email):
	global cache
	if not cache.has_key(email):
		c.execute("SELECT * FROM virtual_aliases WHERE source LIKE %s", (email,))
		r = c.fetchall()
		cache[email] = set([a[2] for a in r])
	return {email:[get_tree(m) for m in cache[email]]}

def del_link(from_email, to_email):
	global c
	global db
	c.execute("DELETE FROM virtual_aliases WHERE source LIKE %s and destination LIKE %s",
		(from_email, to_email))
	affected_rows = db.affected_rows()
	db.commit()
	return affected_rows

def add_link(from_email, to_email):
	global c
	global db
	c.execute("INSERT INTO virtual_aliases (source, destination) VALUES (%s, %s)", 
		(from_email, to_email))
	affected_rows = db.affected_rows() 
	db.commit()
	return affected_rows

def print_tree(tree, level=0, indent="    "):
	for m in tree.keys():
		print (level*indent)+m
		for a in tree[m]:
			print_tree(a, level+1)

if __name__ == "__main__":
	if not DBPASSWD:
		DBPASSWD = raw_input("Password: ")
	db = MySQLdb.connect(user=DBUSER, passwd=DBPASSWD, db=DBNAME)
	c = db.cursor()
	if len(sys.argv) == 2:
		# sys.argv[0] email
		# print full tree
		print_tree(get_tree(expand_email(sys.argv[1])))
		sys.exit(1)
	elif len(sys.argv) == 4:
		# sys.argv[0] cmd source_email dest_email
		# Add or remove the link between source_email and dest_email
		if sys.argv[1] == "add":
			sys.exit(add_link(expand_email(sys.argv[2]), expand_email(sys.argv[3])))
		elif sys.argv[1] == "del":
			sys.exit(del_link(expand_email(sys.argv[2]), expand_email(sys.argv[3])))
		else:
			print "Unknown command \"%s\". Did you mean \"add\" or \"del\"?"
			sys.exit(-1)
	db.close()
