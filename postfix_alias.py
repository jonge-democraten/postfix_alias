#!/usr/bin/env python

import os,sys
import MySQLdb
import string
import re
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

def make_units(unit):
	global c
	global db
	c.execute("SELECT source, destination FROM virtual_aliases WHERE source LIKE '%%.%s@jongedemocraten.nl'" % unit)
	rows = c.fetchall()
	if len(rows) < 1:
		print "Unit not found"
		return -1
	newlinks = {}
	for r in rows:
		# Replace function.unit@jongedemocraten.nl with function@jdunit.nl
		unitsrc = re.sub(r"(.*)\.%s@jongedemocraten.nl" % unit, r"\1@jd%s.nl" % unit, r[0])
		newlinks[unitsrc] = r[0]
	# Only create new aliases if no existing alias exists yet
	sources = set(newlinks.keys())
	c.execute("SELECT source FROM virtual_aliases WHERE source IN ('%s')" % string.join(sources, "', '"))
	rows = c.fetchall()
	srcexist = set()
	for r in rows:
		srcexist.add(r[0])
	#query = "INSERT INTO virtual_aliases (source, destination) VALUES "
	#for s in sources - srcexist:
	#	query += "('%s', '%s'), " % (s, newlinks[s])
	#query = query.rstrip(", ") + ";"
	#print query
	srcmake = sources - srcexist
	if len(srcmake) < 1:
		print "Nothing to do"
		return -1
	a = []
	for s in srcmake:
		a.append( (s, newlinks[s]) )
	c.executemany("INSERT INTO virtual_aliases (source, destination) VALUES (%s, %s)", a)
	c.execute("SELECT source, destination FROM virtual_aliases WHERE source IN ('%s')" % string.join(srcmake, "', '"))
	rows = c.fetchall()
	print "Will create following aliases:"
	for r in rows:
		print "%s => %s" % (r[0], r[1])
	if raw_input("Proceed? [y/N] ").startswith(("y","Y","j","J")):
		db.commit()
		print "Committed"
		return 0
	else:
		db.rollback()
		print "Rolled back"
		return -1

	
def print_usage():
	print """\
Usage:

postfix_alias
	Finds and displays aliases that lead nowhere.
postfix_alias address@jongedemocraten.nl
	Displays the alias tree for the specified address.
postfix_alias unit unitname
	Creates jdunit.nl aliases, asks for confirmation first.
postfix_alias add source@jongedemocraten.nl destination@jongedemocraten.nl
	Adds a new alias.
postfix_alias del source@jongedemocraten.nl destination@jongedemocraten.nl
	Removes an existing alias."""


if __name__ == "__main__":
	if not DBPASSWD:
		DBPASSWD = raw_input("Password: ")
	db = MySQLdb.connect(user=DBUSER, passwd=DBPASSWD, db=DBNAME)
	c = db.cursor()
	c.execute("START TRANSACTION")
	if len(sys.argv) == 1:
		expr = string.join(get_open_leaves(), "', '")
		c.execute("SELECT source, destination FROM virtual_aliases WHERE destination IN ('%s')" % expr)
		print "Aliases going nowhere:"
		for a in c.fetchall():
			print "%s => %s" % (a[0], a[1])
	elif len(sys.argv) == 2:
		# sys.argv[0] email
		# print full tree
		print_tree(get_tree(expand_email(sys.argv[1])))
		sys.exit(1)
	elif len(sys.argv) == 3:
		if sys.argv[1] == "unit":
			sys.exit(make_units(sys.argv[2]))
		else:
			print_usage()
			sys.exit(-1)
	elif len(sys.argv) == 4:
		# sys.argv[0] cmd source_email dest_email
		# Add or remove the link between source_email and dest_email
		if sys.argv[1] == "add":
			sys.exit(add_link(expand_email(sys.argv[2]), expand_email(sys.argv[3])))
		elif sys.argv[1] == "del":
			sys.exit(del_link(expand_email(sys.argv[2]), expand_email(sys.argv[3])))
		else:
			print_usage()
			sys.exit(-1)
	else:
		print_usage()
		sys.exit(-1)
	db.close()
