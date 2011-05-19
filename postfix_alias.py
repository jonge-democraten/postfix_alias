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
domcache = {}
rdomcache = {}


def expand_email(e):
	return e if "@" in e else e+"@jongedemocraten.nl"

def parse_email(e):
	# Split user@example.net to user and example.net
	user, domain = re.split("@", e, 1)
	if domain in rdomcache:
		domid = rdomcache[domain]
		return user, domain, domid
	else:
		return user, domain, None

def get_tree(email, d = 0):
	if d > 64: # Max recursion depth exceeded
		return {email:["!"]}
	global cache
	global c
	if not cache.has_key(email):
		user, domain, domid = parse_email(email)
		c.execute("SELECT destination FROM virtual_aliases WHERE sourceuser=%s AND sourcedomain=%s", (user, domid))
		#c.execute("""SELECT destination FROM virtual_aliases WHERE sourceuser LIKE %s AND sourcedomain IN (
		#		SELECT parent FROM domain_clones WHERE child IN (
		#			SELECT id FROM virtual_domains WHERE name=%s ) )""", (user, domain) )
		rows = c.fetchall()
		cache[email] = set([r[0] for r in rows])
	return (email, [get_tree(m, d+1) for m in cache[email]])


def del_link(user, domid, to_email):
	global c
	global db
	c.execute("DELETE FROM virtual_aliases WHERE sourceuser=%s AND sourcedomain=%s AND destination=%s",
		(user, domid, to_email))
	affected_rows = db.affected_rows()
	db.commit()
	return affected_rows

def add_link(user, domid, to_email):
	global c
	global db
	c.execute("INSERT INTO virtual_aliases (sourceuser, sourcedomain, destination) VALUES (%s, %s, %s)", 
		(user, domid, to_email))
	affected_rows = db.affected_rows() 
	db.commit()
	return affected_rows

def print_tree(tree, level=0, indent="    "):
	if len(cache) <= 1:
		print "Error: Address not found"
	else:
		print (level*indent)+tree[0]
		for a in tree[1]:
			print_tree(a, level+1)

def get_open_leaves():
	global c
	# mailman.jongedemocraten.nl is a virtual alias domain, but is delivered locally
	c.execute("SELECT name FROM virtual_domains WHERE name != 'mailman.jongedemocraten.nl'")
	rows = c.fetchall()
	domains = set()
	for r in rows:
		domains.add("@%s" % r[0])
	# Select all aliases that point to a virtual_alias_domain (i.e. another alias)
	# Don't apply formatting string directly to first argument of c.execute() because of SQL-escaping
	expr = ".*(%s)" % string.join(domains, "|")
	c.execute("SELECT destination FROM virtual_aliases WHERE destination REGEXP %s", expr)
	rows = c.fetchall()
	open_leaves = set()
	for r in rows:
		tree = get_tree(r[0]) # Get tree from the destination
		subtrees = [tree]
		leaves = set()
		while subtrees:
			tree = subtrees.pop()
			if tree[1]: # has leaves
				subtrees += tree[1]
			else:
				leaves.add(tree[0])
		for l in leaves:
			if l.endswith(tuple(domains)):
				open_leaves.add(l)
	return list(open_leaves)

def alias_copy(rows, reorig, rechange):
	global c
	global db
	# reorig and rechange are just strings, not re objects
	if len(rows) < 1:
		print "Error: No unit aliases found"
		return 0
	newalias = {}
	for r in rows:
		# Replace function.unit@jongedemocraten.nl with function@jdunit.nl
		unitsrc = re.sub(reorig, rechange, r[0])
		newalias[unitsrc] = r[0]
	# Only create new aliases if no existing alias exists yet
	sources = set(newalias.keys())
	c.execute("SELECT source FROM virtual_aliases WHERE source IN ('%s')" % string.join(sources, "', '"))
	rows = c.fetchall()
	srcexist = set()
	for r in rows:
		srcexist.add(r[0])
	srcmake = sources - srcexist
	#TODO: delete aliases that should no longer exist
	#srcdel = srcexist - sources
	if len(srcmake) < 1:
		print "Info: Nothing to do"
		return 0
	a = []
	for s in srcmake:
		a.append( (s, newalias[s]) )
	c.executemany("INSERT INTO virtual_aliases (source, destination) VALUES (%s, %s)", a)
	c.execute("SELECT source, destination FROM virtual_aliases WHERE source IN ('%s')" % string.join(srcmake, "', '"))
	rows = c.fetchall()
	print "Will create following aliases:"
	for r in rows:
		print "%s => %s" % (r[0], r[1])
	if raw_input("Proceed? [y/N] ").startswith(("y","Y","j","J")):
		db.commit()
		print "Info: Committed"
		return 1
	else:
		db.rollback()
		print "Info: Rolled back"
		return 0

def make_units(unit):
	global c
	global db
	c.execute("SELECT source, destination FROM virtual_aliases WHERE source LIKE '%%.%s@jongedemocraten.nl'" % unit)
	# alias_copy(rows, reorig, rechange)
	return alias_copy(c.fetchall(), r"(.*)\.%s@jongedemocraten\.nl" % unit, r"\1@jd%s.nl" % unit)

def domain_cache():
	global c
	global domcache
	global rdomcache
	canoncache = {}
	c.execute("SELECT child, parent FROM domain_clones WHERE child!=parent")
	for child, parent in c.fetchall():
		canoncache[child] = parent
	c.execute("SELECT id, name FROM virtual_domains")
	for id, name in c.fetchall():
		domcache[id] = name
		# if domain is just an alias of another domain, rdomcache value is id of real domain not alias
		rdomcache[name] = canoncache[id] if id in canoncache else id
	return 1

def print_usage():
	print """\
Usage:

postfix_alias
	Finds and displays aliases that lead nowhere.
postfix_alias address@jongedemocraten.nl
	Displays the alias tree for the specified address.
postfix_alias unit unitname
	Creates jdunit.nl aliases, asks for confirmation first.
	e.g. postfix_alias unit twente
postfix_alias add source@jongedemocraten.nl destination@jongedemocraten.nl
	Adds a new alias.
postfix_alias del source@jongedemocraten.nl destination@jongedemocraten.nl
	Removes an existing alias."""
	return


if __name__ == "__main__":
	if not DBPASSWD:
		DBPASSWD = raw_input("Password: ")
	db = MySQLdb.connect(user=DBUSER, passwd=DBPASSWD, db=DBNAME)
	c = db.cursor()
	c.execute("START TRANSACTION")
	domain_cache()
	if len(sys.argv) == 1:
		for l in get_open_leaves():
			c.execute("SELECT sourceuser, sourcedomain FROM virtual_aliases WHERE destination=%s", (l,))
			rows = c.fetchall()
			for r in rows:
				# output: alias@jdafdeling.nl doodlopend@jongedemocraten.nl
				print "%s@%s %s" % (r[0], domcache[r[1]], l)
	elif len(sys.argv) == 2:
		# sys.argv[0] email
		# print full tree
		print_tree(get_tree(expand_email(sys.argv[1])))
		sys.exit(0)
	elif len(sys.argv) == 3:
		if sys.argv[1] == "unit":
			sys.exit( 0 if make_units(sys.argv[2]) else 1 )
		else:
			print_usage()
			sys.exit(127)
	elif len(sys.argv) == 4:
		# sys.argv[0] cmd source_email dest_email
		# Add or remove the link between source_email and dest_email
		if sys.argv[1] == "add":
			user, domain, domid = parse_email(sys.argv[2])
			if domid:
				add_link(user, domid, expand_email(sys.argv[3]))
				print "OK"
				sys.exit(0)
			else:
				print "Error: Domain-part of email-address not recognised, add domain first"
				sys.exit(1)
		elif sys.argv[1] == "del":
			user, domain, domid = parse_email(sys.argv[2])
			if domid:
				del_link(user, domid, expand_email(sys.argv[3]))
				print "OK"
				sys.exit(0)
			else:
				print "Error: Domain-part of email-address not recognised, add domain first"
				sys.exit(1)
		else:
			print_usage()
			sys.exit(127)
	else:
		print_usage()
		sys.exit(127)
	db.close()
