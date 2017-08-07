<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>

<div class="data_list">
		<div class="diary_datas">
				<table class="table">
					<tr>
						<th>${fieldNames.get(0)}</th>
						<th>${fieldNames.get(1)}</th>
					</tr>
					<c:forEach var="diary" items="${diaryList }" begin="${beginPage}" end="${endPage}">
						<tr>
							<td><span>&nbsp;<a href="#">${diary.get(0) }</a></span></td>
							<td><span>&nbsp;<a href="#">${diary.get(1) }</a></span></td>
						</tr>

					</c:forEach>

				</table>
		</div>
		<div aria-label="Page navigation">
			<ul  class="pagination">
				${pageCode }
			</ul>
		</div>
</div>
