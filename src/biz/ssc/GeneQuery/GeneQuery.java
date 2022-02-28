//====================================================================
//
// Application: Gene Query
// Author:      Dan Ouellette
// Description:
//      This Java application connects to public MySQL database 
// 'Ensembl/homo_sapiens_core_104_38' and performs queries on table
// 'gene'.
//
//====================================================================
package biz.ssc.GeneQuery;

//Import classes
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//====================================================================
//class ObjectSerialization
//====================================================================
public class GeneQuery
{
    
    //================================================================
    // Fields
    //================================================================
	private static final String COLFMT1 = "%-12s";
	private static final String COLFMT2 = "%-14s";
	private static final String COLFMT3 = "%10s";
	private static final String COLFMT4 = "%-1s";

	//----------------------------------------------------------------
	// printCursorFormatted
	//----------------------------------------------------------------
	public static void printCursorFormatted(ResultSet inResult)
	{
	 	
		// Declare variables
		int dbResultSize = 0;

	    // Print column headings
    	System.out.printf(
			COLFMT1 + COLFMT2 + COLFMT3 + COLFMT3 + "  " + COLFMT4 + 
			"%n", 
			"ID", "Bio Type", "Regn Start", "Regn End", 
			"Description (truncated)");
    	
    	// Attempt to print rows
    	try
		{
    		
    		// Loop to print rows
			while(inResult.next())
			{
				System.out.printf(COLFMT1, inResult.getInt(1));
				System.out.printf(COLFMT2, inResult.getString(2));
				System.out.printf(COLFMT3, inResult.getInt(3));
				System.out.printf(COLFMT3, inResult.getInt(4));
				System.out.print("  ");
				System.out.printf(COLFMT4, 
					inResult.getString(5).substring(24, 66));
				System.out.println();
				dbResultSize = inResult.getRow();
				
			}
	    	
	    	// Print row count
	    	System.out.println("Rows in result: " + dbResultSize);

		} 
    	catch (SQLException e)
	    {
	    	System.out.println("Error accessing database resultset.");
	    	System.out.println("Error message: " + e.getMessage());
    	}
    	
	 }

	//----------------------------------------------------------------
	// printCursorUnformatted
	//----------------------------------------------------------------
	public static void printCursorUnformatted(ResultSet inResult)
	{
	 	
		// Declare variables
		int dbResultSize = 0;

    	// Attempt to print rows
    	try
		{

    	    // Loop to print rows
    		while(inResult.next())
			{
				for (int i = 1; i <= 6; i++)
					System.out.print(inResult.getString(i) + 
						(i < 6 ? ", " : ""));
				System.out.println();
				dbResultSize = inResult.getRow();
			}
	    	
	    	// Print row count
        	System.out.println("Rows in result: " + dbResultSize);

		}
    	catch (SQLException e)
	    {
	    	System.out.println("Error accessing database resultset.");
	    	System.out.println("Error message: " + e.getMessage());
    	}

	}

	//----------------------------------------------------------------
	// main
	//----------------------------------------------------------------
	public static void main(String[] args)
	{
 	
		// Declare variables
		String dbHost = "useastdb.ensembl.org";
		String dbUser = "anonymous";
		String dbPassword = "";
		String db = "homo_sapiens_core_104_38";
		Connection dbConnection;
		Statement dbStatement;
		ResultSet dbResult;
		int dbResultSize = 0;

	    // Show application header
	    System.out.println("Welcome to Gene Query");
	    System.out.println("---------------------\n");
	    
	    // Attempt to connect to database
	    try
	    {
	    	Class.forName("com.mysql.jdbc.Driver");
	    	dbConnection=DriverManager.getConnection(
	    			"jdbc:mysql://" + dbHost + ":3306/" + db, 
	    			dbUser, dbPassword);
	    	System.out.println("Connected to database '" + db + "'.");
	    	
	    	// Define database statement
	    	dbStatement = dbConnection.createStatement();
	    	
			// Show gene table attributes
	    	System.out.println("\nGene Table Attributes");
	    	dbResult = dbStatement.executeQuery("desc gene");
	    	printCursorUnformatted(dbResult);
	    	
			// Show gene table attributes
	    	System.out.println("\nGene Table Query (5 fields and " +
    			"25 rows selected, formatted output)");
	    	dbResult = dbStatement.executeQuery(
    			"select gene_id, biotype, seq_region_start, " +
    			"seq_region_end, description from gene limit 25");
	    	printCursorFormatted(dbResult);
	    	
	    	// Close objects
	    	dbConnection.close();
	    	
    	}
	    catch (Exception e)
	    {
	    	System.out.println("Error connecting to database '" + db + "'.");
	    	System.out.println("Error message: " + e.getMessage());
    	}  
	
		// Show application close
		System.out.println("\nEnd of Gene Query");

	}

}