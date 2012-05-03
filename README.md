vigsql
======

VigSQL embedded SQL interface for Scala.

object TestSQL extends ToySQL {
	def main(args:Array[String]) = {
		EXEC (CREATE TABLE 'dept ('dno INTEGER, 'dname CHAR(10)))
		EXEC (INSERT INTO 'dept VALUES(100, "RnD"))
		EXEC (INSERT INTO 'dept VALUES(300, "Sales"))
		EXEC (INSERT INTO 'dept VALUES(400, "Sales"))
	  
		EXEC (CREATE TABLE 'emp ('eno INTEGER, 'dno INTEGER, 'name CHAR(10)))
		EXEC (INSERT INTO 'emp VALUES(1, 100, "kmkim"))
		EXEC (INSERT INTO 'emp VALUES(2, 200, "jdlee"))
		EXEC (INSERT INTO 'emp VALUES(3, 100, "kumdory"))
		EXEC (INSERT INTO 'emp VALUES(4, 100, "wegra"))
		
		EXEC (SELECT ('name) FROM 'emp WHERE ('dno IN (SELECT ('dno) FROM 'dept)) )	
		
		EXEC (SELECT ('eno, 'dname, 'name) 
		      FROM ('emp INNER_JOIN 'dept )
		      ORDER_BY ('eno ASC))
		
		EXEC (SELECT ('eno, 'name) FROM 'emp ORDER_BY ('eno ASC)) 
		EXEC (SELECT ('eno, 'name) FROM 'emp ORDER_BY ('eno DESC)) 

	  	EXEC (DELETE FROM 'emp WHERE 'eno < 3) 
	  	EXEC (SELECT ('eno, 'name) FROM 'emp WHERE 'eno === 4)
	  	
	  	EXEC (DELETE FROM 'emp)
	  	EXEC (DROP TABLE 'emp)
	  	EXEC (DROP TABLE 'dept)
	}
}