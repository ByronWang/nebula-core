[#ftl/]

	<table class="table table-striped table-bordered table-hover">
		<thead>
			<tr>
				<th class="id">#</th>
				[#list type.fields as field][#if !field.array && !field.ignorable]
					[#switch field.refer]
					[#case "ByVal"]
						[#if !field.key || field.type.name!="ID"]
				<th>${field.displayName}</th>
						[/#if]
						[#break]
						
					[#case "Inline"]
						[#if field.key || field.core][#list field.type.fields as rF][#if field.key && rF.key]
				<th>${field.displayName}&nbsp;${rF.displayName}</th>
						[/#if][/#list][/#if]
						[#break]
						
					[#case "ByRef"]
				<th>${field.displayName}</th>
						[#break]
						
					[#case "Cascade"]
				<th>${field.displayName}</th>
						[#break]
					[/#switch]
				[/#if][/#list]
			</tr>
		</thead>
		<tbody>
			<tr x-ng-repeat="data in datalist | filter:query | orderBy:orderProp">
			<td class="id">{{$index+1}}</td>
				[#assign keyfieldname][/#assign]
			[#list type.fields as field][#if !field.array  && !field.ignorable]
				[#switch field.refer]
				[#case "ByVal"]
					[#if field.key]
						[#if field.type.name!="ID"]
			<td><a href="#/d/${type.name}/{{data.${field.name}}}">{{data["${field.name}"]}}</a></td>
						[/#if]
						[#assign keyfieldname]${field.name}[/#assign]			
					[#elseif field.core]	
			<td><a href="#/d/${type.name}/{{data.${keyfieldname}}}">{{data["${field.name}"]}}</a></td>
					[#else]	
			<td>{{data["${field.name}"]}}</td>
					[/#if]
					[#break]
				[#case "Inline"]
					[#if field.key || field.core][#list field.type.fields as rF][#if field.key && rF.key]
						<td>{{ data["${field.name}${rF.name}"] }}</td>
					[/#if][/#list][/#if]
					[#break]
				[#case "ByRef"]
				[#case "Cascade"]
					<td>[#list field.type.fields as rF]
						[#if field.key && rF.key && rF.name!="ID"]
						{{ data["${field.name}${rF.name}"] }}&nbsp;
						[#elseif rF.key && rF.key && rF.name!="ID"]
						{{ data["${field.name}${rF.name}"] }}&nbsp;
						[#elseif rF.core]
						{{ data["${field.name}${rF.name}"] }}&nbsp;
						[/#if]
					[/#list]</td>
					[#break]
				[/#switch]
			[/#if][/#list]
			</tr>
		</tbody>
	</table>