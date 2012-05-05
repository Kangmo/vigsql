/**
 * ToySQL by Kangmo Kim at ThankyouSoft
 * 
 * Dual License. 
 * 1) GPL v2 ( Free. If you can open your source code. )
 * 2) Commercial License ( Contact me by kangmo.kim@ThankyouSoft.com. If you don't want to open your source code. ) 
 */
import scala.collection.mutable.HashMap;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.ListBuffer;
class ToySQL {
  	class SyntaxError(message:String) extends Exception {
  	    override def toString() = message
  	}
  	
	abstract class SqlType
	case class IntegerType extends SqlType
	case class CharType(max:Int) extends SqlType
	
	abstract class SqlValue {
		def === (rhs:SqlValue) : Boolean
		def != (rhs:SqlValue) : Boolean
		def < (rhs:SqlValue) : Boolean
		def > (rhs:SqlValue) : Boolean
		def >= (rhs:SqlValue) : Boolean
		def <= (rhs:SqlValue) : Boolean
/*	case classes implement 'equals'...no need to define ours.	
		override def equals(any:Any) : Boolean = {
			if ( any.isInstanceOf[SqlValue] ) {
				val another = any.asInstanceOf[SqlValue];
				return this === another;
			}
			return false;
		}
*/		
	}
	case class BooleanValue(value:Boolean) extends SqlValue {
		def === (rhs:SqlValue) = rhs match {
			case rhs_b : BooleanValue => (value == rhs_b.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int === UnsupportedType ")
		}
		def != (rhs:SqlValue) = rhs match {
			case rhs_b : BooleanValue => (value != rhs_b.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int != UnsupportedType ")
		}
		def < (rhs:SqlValue) : Boolean = {
			throw new SyntaxError("Boolean does not support < Operator")
		}
		def > (rhs:SqlValue) : Boolean = {
			throw new SyntaxError("Boolean does not support > Operator")
		}
		def <= (rhs:SqlValue) : Boolean = {
			throw new SyntaxError("Boolean does not support <= Operator")
		}
		def >= (rhs:SqlValue) : Boolean = {
			throw new SyntaxError("Boolean does not support >= Operator")
		}
	}
	case class IntValue(value:Int) extends SqlValue {
		def === (rhs:SqlValue) = rhs match {
			case rhs_i : IntValue => (value == rhs_i.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int === UnsupportedType ")
		}
		def != (rhs:SqlValue) = rhs match {
			case rhs_i : IntValue => (value != rhs_i.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int != UnsupportedType ")
		}
		def < (rhs:SqlValue) = rhs match {
			case rhs_i : IntValue => (value < rhs_i.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int < UnsupportedType ")
		}
		def > (rhs:SqlValue) = rhs match {
			case rhs_i : IntValue => (value > rhs_i.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int > UnsupportedType ")
		}
		def >= (rhs:SqlValue) = rhs match {
			case rhs_i : IntValue => (value >= rhs_i.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int >= UnsupportedType ")
		}
		def <= (rhs:SqlValue) = rhs match {
			case rhs_i : IntValue => (value <= rhs_i.value)
			case _ => throw new SyntaxError("Unsupported Operator : Int <= String ")
		}	  
	}
	case class CharArrayValue(value:String) extends SqlValue {
		def === (rhs:SqlValue) = rhs match {
			case rhs_s : CharArrayValue => (value == rhs_s.value)
			case _ => throw new SyntaxError("Unsupported Operator : String === UnsupportedType ")
		}
		def != (rhs:SqlValue) = rhs match {
			case rhs_s : CharArrayValue => (value != rhs_s.value)
			case _ => throw new SyntaxError("Unsupported Operator : String != UnsupportedType ")
		}
		def > (rhs:SqlValue) = rhs match {
			case rhs_s : CharArrayValue => (value > rhs_s.value)
			case _ => throw new SyntaxError("Unsupported Operator : String > UnsupportedType ")
		}
		def < (rhs:SqlValue) = rhs match {
			case rhs_s : CharArrayValue => (value < rhs_s.value)
			case _ => throw new SyntaxError("Unsupported Operator : String < UnsupportedType ")
		}
		def >= (rhs:SqlValue) = rhs match {
			case rhs_s : CharArrayValue => (value >= rhs_s.value)
			case _ => throw new SyntaxError("Unsupported Operator : String >= UnsupportedType ")
		}
		def <= (rhs:SqlValue) = rhs match {
			case rhs_s : CharArrayValue => (value <= rhs_s.value)
			case _ => throw new SyntaxError("Unsupported Operator : String <= UnsupportedType ")
		}
	}
	

	case class FieldDef(name:Symbol, ty:SqlType)

	abstract class FieldValue {
		def getValue() : SqlValue
	}
	case class IntegerFieldValue(value:Int) extends FieldValue {
		def getValue() : SqlValue = IntValue(value)
	}
	case class CharFieldValue(value:String) extends FieldValue {
	  	def getValue() : SqlValue = CharArrayValue(value)
	}

	case class TableRow(fieldValues : Array[FieldValue])
	{
		def getFieldValue(fieldIndex : Int)
			= fieldValues(fieldIndex);
		override def toString() = fieldValues.mkString(",")
	}

	case class Table(fields:Array[FieldDef]) {
		private val fieldIndexMap = HashMap[Symbol,Int]();
		private var i : Int = 0
		fields.foreach {
			fd => fieldIndexMap(fd.name) = i; i+=1
		}

		private var rows = ListBuffer[TableRow]();
		def setRows(rs:ListBuffer[TableRow]) = rows = rs
		
		def getFieldIndex(fname:Symbol) = fieldIndexMap(fname)
		def delete(pred:AbstractPred) = rows = rows.filter { 
			row => if (pred == null) false else pred.eval(this, row) === BooleanValue(false)
		} ; ()

		def select(projection:Array[Symbol], pred:AbstractPred, groupFieldNames:Array[Symbol]) : ListBuffer[TableRow] = {
			val filteredRows = rows filter {
				row => pred == null || pred.eval(this, row) === BooleanValue(true)
			} 
		    
			if ( groupFieldNames != null ) {
				if ( projection == null )
					throw new SyntaxError("Missing projection list on a query with GROUP BY clause")
				
				val groupigTable = GroupingTable(fields, filteredRows );
				groupigTable.groupRows(groupFieldNames, projection);
				return groupigTable.select(projection, null);

				throw new SyntaxError("GROUP BY feature is not supported yet.")
			}

			var projectedRows = filteredRows
			if (projection != null) {
				val fieldIndexes = projection.map( getFieldIndex(_) )
				projectedRows = ListBuffer[TableRow]();
				for( row <- filteredRows ) {
					val projRow = ArrayBuffer[FieldValue]()
					for ( fi <- fieldIndexes ) {
						projRow += row.fieldValues(fi)
					}
					projectedRows += TableRow(projRow.toArray)
				}
			}
		    
		    return projectedRows
		}
		def insert(r:TableRow) { rows += r }
		def insert(values:Array[Any]) { insert(TableRow(toValueArray(values))) }
		private def toValueArray(values:Array[Any]) : Array[FieldValue] = values.map(
		  _ match { 
		  	case iVal:Int => IntegerFieldValue(iVal)
		  	case sVal:String => CharFieldValue(sVal)
		  	case unknown:Any => { 
		  	  throw new SyntaxError("Unsupported SQL Type : " + unknown.getClass.getSimpleName + ", Value : "+unknown)
		  	}
		  }
		).toArray
	}

	case class GroupingTable(inputFields:Array[FieldDef], inputRows:ListBuffer[TableRow]) {
	    assert(inputFields != null)
	    assert(inputRows   != null)
	    
	    // BUGBUG : duplicate code with Table case class.
		private val fieldIndexMap = HashMap[Symbol,Int]();
		private var i : Int = 0
		inputFields.foreach {
			fd => fieldIndexMap(fd.name) = i; i+=1
		}

	    case class GroupKey(var groupKeys : Array[FieldValue]) {
	    	//kept for {groupFieldNames}
	    	
	    	// need to define comparison operators for GroupKey
	    	override def hashCode : Int = {
	    	  	var sum = 0
	    	  	// ASSUME : FieldValue should be a case class, so hashCode is implemented automatically.
	    		groupKeys.foreach( sum += _.hashCode() )
	    		sum
	    	}
	    	// See if all SqlValues in the groupKeys are exactly same!
	    	override def equals(any:Any) : Boolean = {
	    		if ( any.isInstanceOf[GroupKey] )
	    		{
	    			val another = any.asInstanceOf[GroupKey];
	    			if ( groupKeys.size == another.groupKeys.size) 
	    			{
	    				for(my <- groupKeys; other <- another.groupKeys ) {
				  	    	// ASSUME : FieldValue should be a case class, so equals is implemented automatically.
	    					if ( my != other)
	    					  return false;
	    				}
	    				return true;
	    			}
	    		}
	    		return false;
	    	}
	    }
	    case class FieldKeepings {
//	    	var minValue : SqlValue 
//	    	var maxValue : SqlValue
//	    	var sumValue : SqlValue
	    	
	    }
	    
	    case class GroupKeepings {
	    	// kept for each field in {inputFields} - {groupFieldNames}
	    	var fieldKeepings = Array[FieldKeepings]() 
	    	private var rowCount : BigInt = 0
	    	def incRowCount() { rowCount +=1 }
	    }

	    var groupedFieldNames : Array[Symbol] = null 
	    val groupedMap = HashMap[GroupKey, GroupKeepings]()
		
	    def getGroupKey(row : TableRow, groupFieldNames:Array[Symbol]) = {
	    	GroupKey( groupFieldNames 
	    					map { fieldIndexMap(_) } // convert field names to to field indexes 
	    					map { row.getFieldValue(_) } // convert field indexes to SqlValue of the field in the row 
	    					toArray ) 
	    }
	    
	    def updateGroupKeepings(groupKeepings : GroupKeepings, row : TableRow, projections:Array[Symbol]) {
	    	groupKeepings.incRowCount()
	    }
	    
	    def getGroupKeepings(groupKey : GroupKey ) = {
	    	var groupKeepings : GroupKeepings = null
	    	if (groupedMap.contains(groupKey)) {
	    		groupKeepings = groupedMap(groupKey)
	    	}
	    	else {
	    		groupKeepings = GroupKeepings()
	    		groupedMap(groupKey) = groupKeepings;
	    	}
	    	groupKeepings
	    }
	    
	    // Populate groupedMap based on the groupFields and projections
	    def groupRows(groupFieldNames:Array[Symbol], projections:Array[Symbol]) {
		    assert(groupFieldNames != null)
		    assert(projections != null)

		    // Initialize the grouped result set
		    groupedFieldNames = groupFieldNames
		    groupedMap.clear()
		    
		    for (row <- inputRows) {
		    	val groupKey = getGroupKey(row, groupFieldNames)
		    	val groupKeepings = getGroupKeepings(groupKey)
		    	updateGroupKeepings(groupKeepings, row, projections)
		    }
		}
		
	    // Populate grouped rows based on groupedMap applying projection and HAVING predicate.
		def select(projection:Array[Symbol], havingPred:AbstractPred) : ListBuffer[TableRow] = {
		  	val groupedFieldIndexMap = HashMap[Symbol,Int]();
		  	var i : Int = 0
		  	groupedFieldNames.foreach {
		  		fname => groupedFieldIndexMap(fname) = i; i+=1
		  	}
		  	// HAVING predicate is not supported yet
		  	assert(havingPred == null)
		  	
		  	val fieldIndexes = projection.map( groupedFieldIndexMap(_) )
			val projectedRows = ListBuffer[TableRow]();
				for( group <- groupedMap ) {
					val groupKey = group._1
					val groupKeepings = group._2
					
					val projRow = ArrayBuffer[FieldValue]()
					for ( fi <- fieldIndexes ) {
						projRow += groupKey.groupKeys(fi)
					}
					projectedRows += TableRow(projRow.toArray)
				}

			projectedRows
		}
	}

	private val tableMap = HashMap[Symbol,Table]();

	abstract class AbstractExpr {
		// Evaluate if the table row satisfies the predicate.  
		// return
		def eval(t : Table, row : TableRow) : SqlValue
		def ===(rhs:AbstractExpr) = PredEq(this, rhs)
		def !=(rhs:AbstractExpr) = PredNe(this, rhs)
		def <(rhs:AbstractExpr) = PredLt(this, rhs)
		def >(rhs:AbstractExpr) = PredGt(this, rhs)
		def <=(rhs:AbstractExpr) = PredLe(this, rhs)
		def >=(rhs:AbstractExpr) = PredGe(this, rhs)
		def IN(rhs:SubqueryExpr) = PredIn(this, rhs)
	}
	
	abstract class AbstractPred extends AbstractExpr
	case class PredEq(lhs : AbstractExpr, rhs : AbstractExpr) extends AbstractPred {
		//def eval(t : Table, row : TableRow) : SqlValue = BooleanValue(lhs.eval(t,row) === rhs.eval(t,row))
		
		def eval(t : Table, row : TableRow) : SqlValue = {
			val lval = lhs.eval(t,row)
			val rval = rhs.eval(t,row)
			val result = lval == rval
			BooleanValue(result)
		}
		
	}
	case class PredNe(lhs : AbstractExpr, rhs : AbstractExpr) extends AbstractPred {
		def eval(t : Table, row : TableRow) : SqlValue = BooleanValue(lhs.eval(t,row) != rhs.eval(t,row))
	}
	case class PredGt(lhs : AbstractExpr, rhs : AbstractExpr) extends AbstractPred {
		def eval(t : Table, row : TableRow) : SqlValue = BooleanValue(lhs.eval(t,row) > rhs.eval(t,row))
	}
	case class PredLt(lhs : AbstractExpr, rhs : AbstractExpr) extends AbstractPred {
		def eval(t : Table, row : TableRow) : SqlValue = BooleanValue(lhs.eval(t,row) < rhs.eval(t,row))
	}
	case class PredGe(lhs : AbstractExpr, rhs : AbstractExpr) extends AbstractPred {
		def eval(t : Table, row : TableRow) : SqlValue = BooleanValue(lhs.eval(t,row) >= rhs.eval(t,row))
	}
	case class PredLe(lhs : AbstractExpr, rhs : AbstractExpr) extends AbstractPred {
		def eval(t : Table, row : TableRow) : SqlValue = BooleanValue(lhs.eval(t,row) <= rhs.eval(t,row))
	}
	case class SubqueryExpr(selectStmt : SelectFromStmt) {
		val resultSet = selectStmt.getResultSet(null)
		def contains(value:SqlValue) = {
		    assert(!(value==null))
		    // Check if the result set of the subquery has only one column.
		    assert(resultSet.head.fieldValues.size >=1 )
		    if ( resultSet.head.fieldValues.size > 1)
		    	throw new SyntaxError("Subquery should select only one column.")
		    
//		    resultSet.exists( _.getFieldValue(0).getValue() === value ) // type of _ is TableRow which has Array[FieldValue]
		    resultSet.exists( tableRow => { val rowValue = tableRow.getFieldValue(0).getValue(); rowValue === value } ) // type of _ is TableRow which has Array[FieldValue]
		}
	}
	case class PredIn(lhs : AbstractExpr, rhs : SubqueryExpr) extends AbstractPred {
		def eval(t : Table, row : TableRow) : SqlValue = BooleanValue( rhs.contains( lhs.eval(t,row) ) )
	}
	
	abstract class PredAtom extends AbstractExpr
	case class PredAtomField(fname:Symbol) extends PredAtom {
		def eval(t : Table, row : TableRow) : SqlValue = {
			val fi = t.getFieldIndex(fname)
			val fv = row.getFieldValue(fi)
			fv.getValue()
		}
	}
	case class PredAtomConstInt(value:Int) extends PredAtom {
		def eval(t : Table, row : TableRow) : SqlValue = IntValue(value)
	}
	case class PredAtomConstString(value:String) extends PredAtom {
		def eval(t : Table, row : TableRow) : SqlValue = CharArrayValue(value)
	}
	
	// Common Clauses
	
	class ExecEnv;

	
	abstract class Stmt {
		def exec(env:ExecEnv)
	}
	case class CreateTableStmt(tname:Symbol,fields:Array[FieldDef]) extends Stmt
	{
		def exec(env:ExecEnv) = tableMap(tname) = new Table(fields)
	}
	case class DropTableStmt(tname:Symbol) extends Stmt
	{
		def exec(env:ExecEnv) = tableMap -= tname; ()
	}
	case class InsertIntoStmt(tname:Symbol,values:Array[Any]) extends Stmt
	{
		def exec(env:ExecEnv) = {
			val t = tableMap(tname)
			t.insert(values)
		}	
	}
	abstract class FilterableStmt extends Stmt
	{
	    private val self = this 
	  	var filterPred:AbstractPred = null;
		object WHERE  {
			def apply(pred:AbstractPred) : FilterableStmt = {
				filterPred = pred; self
			} 
		}
	}
	
	case class FieldOrderDesc(field:PredAtomField) {
	    var ascendingOrder = true
		def ASC() = { ascendingOrder = true; this }
		def DESC() = { ascendingOrder = false; this }
	}

	case class SelectFromStmt(table:AbstractTable, fields:Array[Symbol]) extends FilterableStmt
	{   
		private val self = this 
	    private var fieldOrders : Array[FieldOrderDesc] = null
	    private var groupFieldNames : Array[Symbol] = null
	    def hasFieldOrders = fieldOrders != null
	    def getResultSet(env:ExecEnv) = {
			val t = table.getTable()
			var rs = t.select(fields, filterPred, groupFieldNames) 
			
			if ( fieldOrders != null) 
				rs = rs.sortWith((r1,r2) => { 
					val lval = fieldOrders(0).field.eval(t,r1); 
					val rval = fieldOrders(0).field.eval(t,r2); 
					if (fieldOrders(0).ascendingOrder) lval < rval else lval > rval
				} )
			rs
		}
		def exec(env:ExecEnv) = {
		    val rs = getResultSet(env)
			rs.foreach(println)
		}
		
		object ORDER_BY {
//			object BY {
				def apply(fields : FieldOrderDesc*) = {
					val ordersArray = fields.toArray
					if ( ordersArray.size > 1 )
						throw new SyntaxError("ORDER BY clause supports only one field");
					fieldOrders = ordersArray; self 
				}
//			}
		}
		
		object GROUP_BY {
//			object BY {
				def apply(fields: Symbol*) = {
					val fnamesArray = fields.toArray
					if ( fnamesArray.size > 1 )
						throw new SyntaxError("GROUP BY clause supports only one field");
					groupFieldNames = fnamesArray; self
				}
//			}
		}
	}
	case class DeleteFromStmt(tname:Symbol) extends FilterableStmt
	{
		def exec(env:ExecEnv) = {
			val t = tableMap(tname)
			t.delete(filterPred)
		}
	}
	
	case class FieldDesc(name:Symbol) {
		def INTEGER = FieldDef(name,IntegerType())
		object CHAR {
			def apply(max:Int) = FieldDef(name,CharType(max))
		}
	}
		
	case class TableDesc(tname:Symbol) {
		def apply(fdef:FieldDef*) = CreateTableStmt(tname,fdef.toArray)
	}


	object CREATE {
		object TABLE {
			def apply(cts:CreateTableStmt) = cts
		}
	}

	object DROP {
		object TABLE {
			def apply( tname: Symbol) = DropTableStmt(tname)
		}
	}

	object INSERT {
	  	case class ValueDesc(tname:Symbol) {
	  		def VALUES(values:Any*) = {
	  			InsertIntoStmt(tname, values.toArray)
	  		}
	  	}

		object INTO {
			def apply(tname:Symbol) = ValueDesc(tname)
		}
	}
	
	abstract class AbstractTable {
		private val self = this 
		def getTable() : Table
		object INNER_JOIN {
			def apply(rhs : AbstractTable) = InnerJoinTable(self, rhs)
		}
	}
	
	case class InnerJoinTable(lhs:AbstractTable, rhs:AbstractTable) extends AbstractTable {
	    def getTable() = {
	        val joinedTable = new Table(lhs.getTable().fields ++ rhs.getTable().fields) 
	    	for( l <- lhs.getTable().select(null,null,null); 
	        	 r <- rhs.getTable().select(null,null,null)) {
	    		// BUGBUG : Need to check join prediciate
	    		joinedTable.insert( TableRow(l.fieldValues ++ r.fieldValues) )
	    	}
	        joinedTable
	    }
	}
    
	case class AtomTable(name:Symbol) extends AbstractTable {
	  	def getTable() = tableMap(name);
	}
	
	object SELECT {
	  	case class SelectClause( fnames : Array[Symbol]) {
	  		object FROM {
	  			def apply(t:AbstractTable) = {
	  			  	SelectFromStmt(t, fnames)
	  			}
	  		}
	  	}

	    def apply( fnames: Symbol*) = SelectClause( fnames.toArray )
	}
	
	object DELETE {
	    object FROM {
	    	def apply( tname: Symbol) = DeleteFromStmt(tname)
	    }
	}
	
	object EXEC {
		def apply(stmt:Stmt) = stmt.exec(null)
	}
	
	implicit def string2AtomConstString(s:String) = PredAtomConstString(s) 
	implicit def sym2FieldDef(s:Symbol) = FieldDesc(s) 
	implicit def sym2TableDesc(tname:Symbol)= TableDesc(tname)
	implicit def sym2AtomField(s:Symbol) = PredAtomField(s) 
	implicit def int2AtomConstInt(i:Int) = PredAtomConstInt(i) 
	implicit def sym2FieldOrderDesc(s:Symbol) = FieldOrderDesc(PredAtomField(s))
	implicit def sym2AtomTable(s:Symbol) = AtomTable(s)
	implicit def sfs2SubqueryExpr(s:SelectFromStmt) = SubqueryExpr(s)
}
// type check of elements in VALUES
// bug : dno not shown in the cross product.
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

		EXEC (SELECT ('dname) FROM 'dept GROUP_BY ('dname) )
		
		EXEC (SELECT ('name) FROM 'emp WHERE ('dno IN (SELECT ('dno) FROM 'dept)) )	
		
		EXEC (SELECT ('eno, 'dname, 'name) 
		      FROM ('emp INNER_JOIN 'dept )
		      ORDER_BY ('eno ASC))
		
	  	EXEC (SELECT ('name) FROM 'emp)	
	  	
		EXEC (SELECT ('eno, 'name) FROM 'emp ORDER_BY ('eno ASC)) 
		EXEC (SELECT ('eno, 'name) FROM 'emp ORDER_BY ('eno DESC)) 

	  	EXEC (DELETE FROM 'emp WHERE 'eno < 3) 
	  	EXEC (SELECT ('eno, 'name) FROM 'emp WHERE 'eno === 4)
	  	
	  	EXEC (DELETE FROM 'emp)
	  	
	  	EXEC (DROP TABLE 'emp)
	  	EXEC (DROP TABLE 'dept)
	}
}
