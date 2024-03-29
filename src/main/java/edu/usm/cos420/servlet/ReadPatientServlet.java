package edu.usm.cos420.servlet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.usm.cos420.dao.PatientCloudSqlDao;
import edu.usm.cos420.dao.PatientDao;
import edu.usm.cos420.domain.Patient;


@WebServlet(urlPatterns = {"/read"})
public class ReadPatientServlet extends HttpServlet{
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		Long id = Long.decode(req.getParameter("id"));

		PatientDao dao = null;
		try {
			dao = new PatientCloudSqlDao();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		try {
			Patient patient = dao.readPatient(id);
			req.setAttribute("patient", patient);
			req.setAttribute("page", "view");
			req.getRequestDispatcher("/base.jsp").forward(req, resp);
		} catch (Exception e) {
			throw new ServletException("Error reading patient", e);
		}
	}
}
