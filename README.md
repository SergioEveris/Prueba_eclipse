package com.everis.calidad.dao;

//GUI
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

import com.everis.calidad.general.Configuracion;

public class AccesoBBDD {
  private static Logger logger = Logger.getLogger(AccesoBBDD.class);
	
	/**
	 * Este método crea la conexión a la base de datos
	 * @return Connection con la conexión a la base de datos.
	 * @throws IOException 
	 * @throws Exception 
	 */
	
	
	public static Connection crearConexion() throws SQLException, IOException{
		
		Connection conexion = null;
		if (logger.isDebugEnabled())
			logger.debug("-> crearConexion()");
		Configuracion conf = Configuracion.getInstancia();	
		try {
			String dir = conf.getParametro("dir");
			String port = conf.getParametro("port");
			String user = conf.getParametro("user");
			String pass = conf.getParametro("pass");
			String BBDD = conf.getParametro("BBDD");

			
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			String conex = "jdbc:mysql://"+dir+":"+port+"/"+BBDD;
//				String conex = "jdbc:mysql://5.0.1.1:3306/calidadrRC";
			logger.info("Obteniendo conexion de "+conex);
			conexion = DriverManager.getConnection(conex,user,pass);
			
		}catch (SQLException e){
			logger.error("Error al obtener la conexion.",e);
			throw e;
		}
		
		if (logger.isDebugEnabled()) 
			logger.debug("<- crearConexion()"+conexion);
		return conexion;
	}
	
	/**
	 * Este método cierra la conexión con la base de datos.
	 * @param conexion de tipo Connection con la conexión que queremos cerrar. 
	 * @throws Exception 
	 */
	public static void cerrarConexion(Connection conexion) throws SQLException{
		if (logger.isDebugEnabled()) 
			logger.debug("-> CerrarConexion("+conexion+")");
		if (conexion!=null){
			if (!conexion.isClosed())
				conexion.close();
		}
	}
	
	/**
	 * Funcion que cierra los cursores que se le pasan por parametro.
	 * @param stmt
	 * @param rs
	 * @throws SQLException
	 */
	public static void cerrarCursor(Statement stmt, ResultSet rs)throws SQLException{
		if (rs!=null)
			rs.close();
		if (stmt !=null)
			stmt.close();
		
		stmt=null;
		rs=null;
	}
	
	/**
	 * Este método realiza una consulta que se le pasa por parámetro y devuelve el Resulset para su posterior
	 * procesado.
	 * @param consulta de tipo String con la consulta a ejecutar.
	 * @param conexion de tipo Connection con la conexión a la base de datos.
	 * @param stmt statement para realizar la consulta. Si es null creará uno nuevo de la conexion
	 * @return Resulset con la consulta.
	 * @throws Exception 
	 */
	public static ResultSet ejecutarConsulta(String consulta, Connection conexion, Statement stmt) throws SQLException{
		if (logger.isDebugEnabled()) 
			logger.debug("-> ejecutarConsulta("+consulta+", "+conexion);
		try{
			if (conexion == null)
				conexion=crearConexion();
		}catch (IOException e){
			throw new SQLException ("Error al ejecutar Consulta por no poder crear conexion debido a error en el fichero de configuracion: "+e.getMessage());
		}
		
		ResultSet rs = null;
		try {
			if (stmt==null)
				stmt = conexion.createStatement();
			rs = stmt.executeQuery (consulta);
		} catch (SQLException e) {
			logger.error("Error al ejecutar la consulta "+consulta,e);
			throw e;
		}
		if (logger.isDebugEnabled()) 
			logger.debug("<- ejecutarConsulta():"+rs);
		return rs;
	}

	/**
	 * Este método realiza una consulta que se le pasa por parámetro y devuelve el Resulset para su posterior
	 * procesado.
	 * @param consulta de tipo String con la consulta a ejecutar.
	 * @param conexion de tipo Connection con la conexión a la base de datos.
	 * @throws Exception 
	 */
	public static void ejecutarInsert(String consulta, Connection conexion)throws SQLException{
		Statement stmt = conexion.createStatement();
		ejecutarInsert(consulta, conexion, stmt);
		stmt.close();
		stmt=null;
	}
	
	public static void ejecutarInsert(String consulta, Connection conexion, Statement stmt) throws SQLException{
		if (logger.isDebugEnabled()) 
			logger.debug("-> ejecutarInsert("+consulta+", "+conexion);
		
		try {
			if (stmt==null)
				stmt = conexion.createStatement();
			stmt.executeUpdate (consulta);
		} catch (SQLException e) {
			logger.error("Error al ejecutar la consulta "+consulta,e);
			throw e;
		}
		if (logger.isDebugEnabled()) 
			logger.debug("<- ejecutarInsert()");
	}
}
