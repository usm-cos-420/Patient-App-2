<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn"%>
<div class="container">
	<h3>
		<c:out value="${action}" />
		patient
	</h3>

	<form method="POST" action="${destination}">

		<div class="form-group">
			<label for="firstName">First Name</label> <input type="text"
				name="firstName" id="firstName"
				value="${fn:escapeXml(patient.firstName)}" class="form-control" />
		</div>

		<div class="form-group">
			<label for="lastName">Last Name</label> <input type="text"
				name="lastName" id="lastName"
				value="${fn:escapeXml(patient.lastName)}" class="form-control" />
		</div>

		<div class="form-group">
			<label for="gender">Gender</label> <input type="text"
				name="gender" id="gender" value="${fn:escapeXml(patient.gender)}"
				class="form-control" />
		</div>

		<div class="form-group">
			<label for="address">Address</label> <input type="text"
				name="address" id="address" value="${fn:escapeXml(patient.address)}"
				class="form-control" />
		</div>

		<div class="form-group">
			<label for="birthDate">Date of Birth</label> <input type="date"
				name="birthDate" id="birthDate"
				value="${fn:escapeXml(patient.birthDate)}" class="form-control" />
		</div>

		<div class="form-group hidden">
			<input type="hidden" name="id" value="${patient.id}" />
		</div>

		<button type="submit" class="btn btn-success">Save</button>
	</form>
</div>
