<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn"%>
<div class="container">
	<h3>Patient Information</h3>
	<div class="btn-group">
		<a href="/update?id=${patient.id}" class="btn btn-primary btn-sm">
			<i class="glyphicon glyphicon-edit"></i> Edit patient
		</a> <a href="/delete?id=${patient.id}" class="btn btn-danger btn-sm">
			<i class="glyphicon glyphicon-trash"></i> Delete patient
		</a>
	</div>

	<div class="media">
		<div class="media-body">
			<h4 class="book-title">
				${fn:escapeXml(patient.firstName)} ${fn:escapeXml(patient.lastName)}
				<small>${fn:escapeXml(patient.birthDate)}</small>
			</h4> 
			<p> Gender: ${fn:escapeXml(patient.gender)} </p>
			<p> Address: ${fn:escapeXml(patient.address)} </p>
		</div>
	</div>
</div>
<!-- [END view] -->
