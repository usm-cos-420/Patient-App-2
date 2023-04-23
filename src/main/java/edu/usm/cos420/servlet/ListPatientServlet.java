package edu.usm.cos420.servlet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.usm.cos420.dao.PatientCloudSqlDao;
import edu.usm.cos420.dao.PatientDao;
import edu.usm.cos420.domain.Patient;
import edu.usm.cos420.domain.Result;


@WebServlet(urlPatterns = {"/list"})
public class ListPatientServlet extends HttpServlet {
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		
		PatientDao dao = null;
		try {
			dao = new PatientCloudSqlDao();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	
		String startCursor = req.getParameter("cursor");
		List<Patient> patients = null;
		String endCursor = null;
		try {
			Result<Patient> result = dao.listPatients(startCursor);
			patients = result.result;
			endCursor = result.cursor;
		} catch (Exception e) {
			throw new ServletException("Error listing patients", e);
		}
		
		req.getSession().getServletContext().setAttribute("patients", patients);
		
		req.setAttribute("cursor", endCursor);
	    req.setAttribute("page", "list");

		req.getRequestDispatcher("/base.jsp").forward(req, resp);
	}
}
