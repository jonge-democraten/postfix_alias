#!/usr/bin/env python

import os,sys
import MySQLdb
from optparse import OptionParser

DBNAME = "postfix"
DBUSER = "aliaser"
# trick to read keyfile from same dir as actual script, even when called via symlink
keyfile = os.path.dirname(os.path.realpath(__file__))+"/postfix_alias.key"
f = open(keyfile, "r")
DBPASSWD = f.readline().rstrip()
f.close()


cache = {}
tree = {}


def expand_email(e):
	if e.find("@") == -1:
		return e+"@jongedemocraten.nl"
	return e

def get_tree(email, d = 0):
	if d > 64: # Max recursion depth exceeded
		return {email:["!"]}
	global cache
	global c
	if not cache.has_key(email):
		c.execute("SELECT * FROM virtual_aliases WHERE source LIKE %s", (email,))
		r = c.fetchall()
		cache[email] = set([a[2] for a in r])
	return (email, [get_tree(m, d+1) for m in cache[email]])


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
	print (level*indent)+tree[0]
	for a in tree[1]:
		print_tree(a, level+1)

def get_open_leaves():
	global c
	global db
	c.execute("SELECT * FROM virtual_aliases WHERE destination LIKE '%@jongedemocraten.nl'")
	rows = c.fetchall()
	open_leaves = set()
	for r in rows:
		tree = get_tree(r[2]) # Get tree from the destination
		subtrees = [tree]
		leaves = set()
		while subtrees:
			tree = subtrees.pop()
			if tree[1]: # has leaves
				subtrees += tree[1]
			else:
				leaves.add(tree[0])
		for l in leaves:
			if l.endswith("@jongedemocraten.nl"):
				open_leaves.add(l)
	return list(open_leaves)

	



if __name__ == "__main__":
	if not DBPASSWD:
		DBPASSWD = raw_input("Password: ")
	db = MySQLdb.connect(user=DBUSER, passwd=DBPASSWD, db=DBNAME)
	c = db.cursor()
	if len(sys.argv) == 1:
		print get_open_leaves()
	elif len(sys.argv) == 2:
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
