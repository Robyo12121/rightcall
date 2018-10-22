Ext.onReady(function() {
			// tab des ids de la page en cours et ids selectionnes de la page en
			// cours
			var GRID_PAGING_WIDTH=0.93;
			var countMessage = 0;
			var totalMessage=false;
			var tabPageSelectedIds = [];
			var tabPageIds = [];
			var dlgPopup=null;
			var nbIdsSelected=0;
			var defaultValueResultPerPage=EasyRecord.SEARCH_RESULT_PAGE_SIZE;
			var nbResultPerPage=EasyRecord.SEARCH_RESULT_MESSAGE_PAGE_SIZE;
			if(nbResultPerPageJsp != 0){
				nbResultPerPage=nbResultPerPageJsp;
			}
			
			Ext.tip.QuickTipManager.init();
			// Turn on validation errors beside the field globally.
			Ext.form.Field.prototype.msgTarget = "side";

			// setup the state provider, all state information will be saved to a cookie
		    Ext.state.Manager.setProvider(Ext.create('Ext.state.CookieProvider'));
		    
		    //Check if a state was saved
		    Ext.stateExists = (Ext.state.Manager.get("messageGrid"+Ext.host)!=null);
		    
			// boutons du formulaires
			var searchButton = new Ext.Button({
				text : Label.valider.toUpperCase(),
				handler : onSearchClicked
			});
			var clearButton = new Ext.Button({
				text : Label.resetform.toUpperCase(),
				handler : onClearClicked
			});

			// definition des fields du formulaire
			// Create a store for filters
			// Add the option "All" at the head of the filter list if exist
			var filterList;
			if(isNoFilter==false){
				Ext.filterList.splice(0, 0, {
					id : "0",
					libelle : accessToRestrictdKeyword ? Label.menu.filter.none.select : Label.menu.filter.all.select
				});
				filterList = new Ext.data.Store({
				data : Ext.filterList,
				root : "result",
				fields : [ "id", "libelle" ]
			});
			} else {
				filterList = new Ext.data.Store({
					data : {'items':[
					   { 'id': "0", 'libelle': accessToRestrictdKeyword ? Label.menu.filter.none.select : Label.menu.filter.all.select }
					]},
					root : "result",
					fields : [ "id", "libelle" ]
				});
			}
			var filtreField = {
				xtype : "combo",
				id : "filtre",
				hidden : isNoFilter,
				fieldLabel : Label.menu.filter.choice,
				labelWidth : 150,
				inputWidth : 550,
				colspan : 4,
				name : "filtre", // Name of field that will receive the value to post.
				store : filterList,
				displayField : "libelle", // Underlying data field name to bind to this ComboBox.
				valueField : "id", // Underlying data value name to bind to this ComboBox.
				triggerAction : "all", // "All" to retrieve all records.
				typeAhead : true,
				forceSelection : false,
		    	selectOnFocus: true,
				queryMode : "local",
				value : "0",
				padding : '5 5 5 0'
			};
			
			var flagFurtifField = {
				xtype : "checkbox",
				id : "flag_furtif",
				colspan : 2,
				padding : '0 0 0 155',
				hidden : !accesToMsgFurtif || globalFurtifOption==Constantes.FURTIF_DISABLED || globalFurtifOption==Constantes.FURTIF_FORCED,
				boxLabel : Label.search.msg.flag.furtif,
				name : "flag_furtif" // Name of field that will receive the
										// value to post.
			};
			
			var flagDelField = {
				xtype : "checkbox",
				id : "flag_del",
				colspan : 2,
				padding : '0 0 0 155',
				hidden : !accessToMsgFlagDelete,
				boxLabel : Label.search.msg.flag.del,
				name : "flag_del" // Name of field that will receive the value
									// to post.
			};
			
			var dateDebutField = {
				xtype : "datefield",
				id : "dateDebut",
				format : Label.format.date.cal,
				fieldLabel : Label.search.msg.date.dispo.debut,
				labelAlign : 'right',
				maxValue : new Date(), // limited to the current date or prior
				name : "dateDebut", // Name of field that will receive the value
									// to post.
				labelWidth : 150,
				value : null,
				inputWidth : 120,
				submitFormat: 'd/m/Y'
			};
			
			var heureDebutField = {
				xtype : "timefield",
				id : "heureDebut",
				labelWidth : 0,
				fieldLabel : "",
				increment : 30,
				name : "heureDebut", // Name of field that will receive the
										// value to post.
				value : null,
				inputWidth : 90,
				submitFormat: 'H:i'
			};
			
			var dateFinField = {
				xtype : "datefield",
				id : "dateFin",
				format : Label.format.date.cal,
				// altFormats:ï¿½Y-m-d H:i:s.uï¿½,
				fieldLabel : Label.search.msg.date.dispo.fin,
				labelAlign : 'right',
				maxValue : new Date(), // limited to the current date or prior
				name : "dateFin", // Name of field that will receive the value
									// to post.
				labelWidth : 20,
				value : null,
				inputWidth : 120,
				submitFormat: 'd/m/Y'
			};
			
			var heureFinField = {
				xtype : "timefield",
				id : "heureFin",
				labelWidth : 0,
				fieldLabel : '',
				increment : 30,
				name : "heureFin", // Name of field that will receive the value
									// to post.
				inputWidth : 90,
				submitFormat: 'H:i'
			};
			
			var datesContainer = {
				xtype : "fieldcontainer",
				colspan : 4,
				layout : "hbox",
				width: "100%",
				items : [ dateDebutField, heureDebutField, dateFinField, heureFinField ]
			};
			
			var checkBoxContainer = {
					xtype : "fieldcontainer",
					colspan : 4,
					layout : "hbox",
					width: "100%",
					padding: "0 0 0 155", 
					items : [flagFurtifField, flagDelField]
				};

			var refSviField = {
				xtype : "textfield",
				colspan : 2,
				id : "refSvi",
				fieldLabel : Label.search.msg.refSvi,
				name : "refSvi", // Name of field that will receive the value
									// to post.
				labelWidth : 150,
				value : null,
				inputWidth : 170
			};
			
			var refErField = {
				xtype : "textfield",
				id : "refEr",
				colspan : 2,
				fieldLabel : Label.search.msg.refEr,
				name : "refEr", // Name of field that will receive the value to
								// post.
				labelWidth : 150,
				value : refEr,
				inputWidth : 170
			};
			
			var listRefErField = {
				xtype : "filefield",
				id : "listRefEr",
				colspan : 2,
				fieldLabel : Label.search.msg.listRefEr,
				name : "listRefEr", // Name of field that will receive the value
									// to post.
				labelWidth : 150,
				buttonText: Label.search.msg.browse.toUpperCase(), //Rajouter un label
				value : null,
				inputWidth : 170,
				listeners: {
			            'change': function(fb, v){
			            	var listRefEr = Ext.getCmp('listRefEr');
							if (listRefEr.getValue()!='') {
								formMessageSearch.getForm().submit({
									url: "Json?req=MESSAGE_SEARCH_UPLOADREF",
									waitMsg: Label.search.msg.uploadMsg,
									method: 'POST',
									success: function(form, action) {
										listRefEr.setRawValue(Label.search.msg.upload.ok);
									},
									failure: function(form, action) {
										EasyRecord.messageError(Label.search.msg.upload.msgKo);
										listRefEr.markInvalid(Label.search.msg.upload.ko);
										listRefEr.setRawValue(Label.search.msg.upload.ko);
									}
								});
							}
			            }
				}
			};

			// Create a store for causes
			var causeList = new Ext.data.Store({
				data : Ext.causeList,
				root : "result",
				fields : [ "id", "libelle" ]
			});
			
			var causeField = {
				xtype : "combo",
				id : "cause",
				colspan : 2,
				fieldLabel : Label.search.msg.causeEnr,
				name : "cause", // Name of field that will receive the value to
								// post.
				store : causeList,
				displayField : "libelle", // Underlying data field name to
											// bind to this ComboBox.
				valueField : "id", // Underlying data value name to bind to
									// this ComboBox.
				triggerAction : "all", // "All" to retrieve all records.
				typeAhead : true,
				forceSelection : false,
				queryMode : "local",
				value : "TOUS",
				labelWidth : 150,
				inputWidth : 200
			// labelAlign: 'left',
			};
			
			var criteresDataField = {
				xtype : "textfield",
				id : "criteresData",
				colspan : 2,
				hidden : !isDataComp,
				fieldLabel : Label.search.msg.data.label,
				name : "criteresData", // Name of field that will receive the
										// value to post.
				labelWidth : 150,
				inputWidth : 170,
				value : null,
				inputAttrTpl : dataCompHelpTip
			/*
			 * tip: dataCompHelpTip, listeners: { render: function(c) {
			 * Ext.create('Ext.tip.ToolTip', { target: c.getEl(), html: c.tip
			 * }); } }
			 */
			};
			
			var criteresField = {
				xtype : "textfield",
				id : "criteres",
				colspan : 2,
				fieldLabel : Label.search.msg.criteres.label,
				name : "criteres", // Name of field that will receive the value
									// to post.
				labelWidth : 150,
				inputWidth : 170,
				value : null,
				inputAttrTpl : criteresHelpTip
			/*
			 * tip: criteresHelpTip, listeners: { render: function(c) {
			 * Ext.create('Ext.tip.ToolTip', { target: c.getEl(), html: c.tip
			 * }); } }
			 */
			};
			// Label fornew line
			var newLine = {
				xtype : 'label',
				id: 'newLine',
				colspan : 4
			}
			var numAppelantField = {
				xtype : 'textfield',
				id : 'numAppelant',
				colspan : 2,
				fieldLabel : Label.search.msg.numAppelant,
				name : "numAppelant", // Name of field that will receive the
										// value to post.
				labelWidth : 150,
				value : null,
				inputWidth : 170
			};
			
			var resetForm = {
					xtype: 'hidden',
					id: 'resetForm',
					name: 'resetForm',
					value: true //only at the first time
			};
			
			// Create a form panel for the search page.
			var formMessageSearch = new Ext.form.Panel({
				id: 'formMessageSearch',
				width: Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
				collapsible : true,
				titleAlign : "left",
				title : Label.menu.message.search.toUpperCase(),
				autoHeight : true,
				cls : "centeredElement",
				ui: 'search',
				layout : {
					type : 'table',
					columns : 4,
					tableAttrs : { align: 'center' }
				},
				defaults : {
					labelAlign : 'right',
					autoHeight : true,
					border : false
				},
				items : [filtreField, refErField, refSviField,
						 listRefErField, flagFurtifField, flagDelField,
						 numAppelantField,criteresField, causeField, criteresDataField, 
						 newLine, datesContainer, resetForm
						],
				buttonAlign : 'center',
				buttons : [ clearButton, {
					xtype : 'tbspacer',
					width : 20
				}, searchButton ],
				// cls: "centeredElement", // Center element horizontally.
				renderTo : "form"
			});
			
			var dynamicResultStoreField=[
			    "msgId", "msgName", 
			    {
			    	name : "dateRec",
			    	type : 'date',
			    	dateFormat : 'time'
			    }, 
			    "numeroAppelant", "duree", "displayType"
   			];
			// Loop keywords		 
			var j=0;
			//alert('Global.nombreMotCle:'+nbMotCleList+'');
			while(j<nbMotCleList){
				dynamicResultStoreField[dynamicResultStoreField.length] = "displayColW"+ String(j+1) ;
			   	j++;
			}
			dynamicResultStoreField[dynamicResultStoreField.length] ="displayDataCompCol";
			dynamicResultStoreField[dynamicResultStoreField.length] ="note";
			dynamicResultStoreField[dynamicResultStoreField.length] ="status";
			dynamicResultStoreField[dynamicResultStoreField.length] ="archivage";
			dynamicResultStoreField[dynamicResultStoreField.length] ="nouveau";
			dynamicResultStoreField[dynamicResultStoreField.length] ="isSelected";

			// Create a store containing result of search.
			var resultStore = new Ext.data.JsonStore(
					{
						proxy : EasyRecord.Ext
								.getProxy("Json?req=MESSAGE_SEARCH"),
						pageSize : nbResultPerPage, // For
																		// paging
						remoteSort : true,
						fields : dynamicResultStoreField,
						sorters : {
							property : 'dateRec',
							direction : 'DESC' // or 'DESC' (case sensitive for
												// local sorting)
						},
						listeners : {
							beforeload : function(thisStore, options) {
								makePageIdTabs(thisStore);
								thisStore.getProxy().extraParams = formMessageSearch
										.getForm().getValues(false);
								if ((!(typeof tabPageSelectedIds === 'undefined'))
										&& tabPageSelectedIds != null
										&& tabPageSelectedIds.length > 0) {
									thisStore.getProxy().extraParams['pageSelectedIds'] = tabPageSelectedIds
											.toString();
								}
								if ((!(typeof tabPageIds === 'undefined'))
										&& tabPageIds != null
										&& tabPageIds.length > 0) {
									thisStore.getProxy().extraParams['pageIds'] = tabPageIds
											.toString();
								}
								searchButton.setDisabled(true);
								clearButton.setDisabled(true);
							},
							load : function(thisStore, records, options) {
								Ext.getCmp('resetForm').setValue(false);
								setSelection(thisStore);
								searchButton.setDisabled(false);
								clearButton.setDisabled(false);
								getNbSelected();
							},
							exception : function(misc) {
								searchButton.setDisabled(false);
								clearButton.setDisabled(false);
							}
						}
					});
					
			
			var legendToolbar = new Ext.toolbar.Toolbar({
				width : Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
				style: {
                   padding: '0'
                },
				items: [{
					xtype: 'label',
					id : 'nbSelectedLabel'
				},'->',
				{
					xtype : "button",
					id : 'fullWindowButton',
					ui: 'icon',					
					scale: 'medium',
					iconCls: 'icon-fullscreen',
					tooltip: Label.msg.fullscreen.tip,
					handler : onFullWindowClicked,
					cls: 'x-btn-toolbar'
				}, '-', 
				{
					xtype : "button",
					id : 'resetViewButton',
					ui: 'icon',					
					scale: 'medium',
					iconCls: 'icon-refresh',
					tooltip: Label.msg.resetview.tip,
					handler : function () {
						Ext.state.Manager.clear('messageGrid'+Ext.host); 
						location.reload();
					},
					cls: 'x-btn-toolbar'
				}, '-', 
				{
					xtype: "button",
					text: Label.msg.legende.title.toUpperCase(),
					handler: displayLegend,
					cls: 'x-btn-toolbar'
				}],
				renderTo : "nbSelected"
		     });	
		     
		   
			var tabResult=[];
			resultMessage = Constantes.NB_RESULT_MESSAGE.split(',');
			for(var i = 0; i <= resultMessage.length; i++){
				tabResult[i] = { 'id' : i, 'valueData': resultMessage[i]};
				if(resultMessage[i]==nbResultPerPage){
					defaultValueResultPerPage = i;
				}
			}
			
			var nbResultStore = Ext.create('Ext.data.Store', {
				storeId:'nbResultStore',
				fields:['id','valueData'],
				data:{'items': tabResult},
				proxy: {
					type: 'memory',
					reader: {
						type: 'json',
						root: 'items'
					}
				}
			});
			
		    var displayNbResultMessage = {
				xtype : "combo",
				id : "displayNbResultMessage",
				fieldLabel : Label.search.msg.msgCount,
				name : "displayNbResultMessage", // Name of field that will receive the value to
								// post.
				store : nbResultStore,
				displayField : "valueData", // Underlying data field name to
											// bind to this ComboBox.
				valueField : "id", // Underlying data value name to bind to
									// this ComboBox.
				triggerAction : "all", // "All" to retrieve all records.
				typeAhead : true,
				value : defaultValueResultPerPage,
				//hidden : !globalScreenRecording || !userConnected.hasAccesToEnregistrementVideo,
				forceSelection : true,
				queryMode : "local",
				labelWidth : 200,
				inputWidth : 60,
				listeners: {
					select: function(thisStore, records, options) {
						nbResultPerPage=this.getDisplayValue();
						// Reload page 1 and send search criteria.
						//alert("Ext.getStore('resultStore').pageSize : " + Ext.getStore('resultStore').pageSize);
						//Ext.getStore('resultStore').pageSize=nbResultPerPage;
						Ext.apply(resultStore, {pageSize: nbResultPerPage});
						var param;

						// Get form fields and store them in the store baseParams
						// attribute for paging
						// to work.
						param = formMessageSearch.getForm().getValues(false);
						param['resetSelectedIds'] = true;
						param['nbResultPerPage'] = nbResultPerPage;
						// Reload page 1 and send search criteria.
						resultStore.loadPage(1, {
							params : param
						});
					}
				}
			// labelAlign: 'left',
			};
			
			// Create Button toolbar
			var buttonToolbar = new Ext.toolbar.Toolbar({
				width : Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
				style: {
                   marginTop: '10px',
                   padding: '0'
                },
                items: ['->', {
                	id: 'labelAction',
					xtype: 'label',
					text: Label.grid.selection.results
				},  {
					xtype : "button",
					id : 'deleteButton',
					text : Label.supprimer.toUpperCase(),
					iconCls : 'delete',
					hidden : !canDelete,
					disabled : true,
					handler : totalDel,
					cls: 'x-btn-toolbar'
				},  '-',  {
					xtype : "button",
					id : 'zipButton',
					text : Label.msg.list.download.zip,
					iconCls : 'zip',
					hidden : !canTelechargMass,
					disabled : true,
					handler : totalZip,
					cls: 'x-btn-toolbar'
				}, //add separators only if previous button is visible
				(canTelechargMass?'-':null),
				{
					xtype : "button",
					id : 'csvButton',
					iconCls : 'zip',
					text : Label.msg.list.download.csv,
					disabled : true,
					handler : totalCsv,
					cls: 'x-btn-toolbar'
				}],
				renderTo : "buttonsToolbar"
		     });
			
			// Create a paging tool bar used in result of the search.
		    // Top paging 
			var topPagingToolbar = new Ext.toolbar.Paging({
				store : resultStore
			});
			
			var topAllPagingToolbar = new Ext.toolbar.Toolbar({
				width : Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
				items: [{
					xtype: 'label',
					id : 'resultLabelId',
					text:  Label.result.toUpperCase()  + ' : ',
					cls :'label-color'
				}, '->', '->', 
				topPagingToolbar, '->',
				displayNbResultMessage
				]
		     });

			// Bottom padging
			var bottomPagingToolbar = new Ext.toolbar.Paging({
				store : resultStore
			});
			
			// Create Button toolbar
			var bottomAllPagingToolbar = new Ext.toolbar.Toolbar({
				width : Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
				items: [{
					xtype: 'label',
					text: '',
					width: 100
				}, '->', '->',
				bottomPagingToolbar, '->',
				{
					xtype: 'label',
					text: '',
					width: 260
				}
				]
		     });
			
			// create tab for dynamique data column +dynamique action col
			// Be careful: you must put data and action column in the same var to work good 
			// first add data columns
			
			// second add action column
			// a. create items for action column
			var addEl=1;
			var actionColumnItems = [	{
				iconCls : 'hp',
				tooltip : Label.ecouter,
				handler : viewItem
			}];
			if (canMail) {
				actionColumnItems[addEl] = {
					xtype : 'tbspacer',
					width : 20
				};
				addEl++;
				actionColumnItems[addEl] = {
					iconCls : 'mail',
					getTip : function(v,meta,r){
						var mediaVal=r.get("displayMedia");
						if(mediaVal!=Constantes.AUDIO){
							return "";
						}else{
							return Label.emailer;
						}
					},
					isDisabled : !canMail,
					handler : mailItem,
					hidden : !canMail,
					getClass : function(v,meta,r){
						var mediaVal=r.get("displayMedia");
						if(mediaVal!=Constantes.AUDIO){
							return "";
						}else{
							return "mail";
						}
					}
				};
				addEl++;
			}
			if (canArchivage) {
				actionColumnItems[addEl] = {
					xtype : 'tbspacer',
					width : 20
				};
				addEl++;
				actionColumnItems[addEl] = {
					iconCls : 'tape',
					isDisabled : function itemTapeDisabled(view,
					 rowIndex, colIndex, item,
					 record) { 
						if (canArchivage) {
							if(record.get("archivage")==0 || record.get("archivage")==1){
								return false ; 
							} else {
								return true ; 
							}
						} else {
							return true;
						}
					},
					hidden : !canArchivage,
					tooltip : Label.searchMsgArchive,
					handler : archiveItem
				};
				addEl++;
			}
			if (accessToMsgFlagDelete) {
				actionColumnItems[addEl] = {
					iconCls : 'deleteFl',
					isDisabled : function itemDelFlDisabled(
							view, rowIndex,
							colIndex, item, record) {
						if (canDelete && accessToMsgFlagDelete) {
							if (record.get("status") == Constantes.STATUS_MSG_FL_DELETE) {
								return false;
							} else {
								return true;
							}
						} else {
							return true;
						}
					},
					hidden : !(canDelete && accessToMsgFlagDelete),
					tooltip : Label.conserver,
					handler : keepItem
				};
				addEl++;
			}
			
			// b. add action col to dynamic col 
			var dynamicCols = [{
				xtype : 'actioncolumn',
				width : EasyRecord.GRID_ACTION_COLUMN_WIDTH/2
						+ (canMail ? (EasyRecord.GRID_ACTION_COLUMN_WIDTH/2)+20 : 0)
						+ (canArchivage ? (EasyRecord.GRID_ACTION_COLUMN_WIDTH/2)+15 : 0)
						+ (accessToMsgFlagDelete ? (EasyRecord.GRID_ACTION_COLUMN_WIDTH/2)+20 : 0),
				sortable : false,
				hideable : false,
				draggable : false,
				locked   : true,
				menuDisabled : true,
				items : actionColumnItems
				},
			  new Ext.grid.Column( {
					id : "colId",
					// text: Label.msg.list.ref,
					dataIndex : "msgId",
					// sortable: true,
					hidden : true,
					isDisabled: true,
					locked   : true,
					hideable : false
					// flex: 12
			  }),
			  new Ext.grid.Column({
					id : "colMsgName",
					text : Label.msg.list.ref,
					dataIndex : "msgName",
					sortable : true,
					hidden : false,
					align: 'center',
					tdCls: 'wrapCell',
					autoSizeColumn: true,
					locked   : true,
					//maxWidth: 170
			   }),
			   new Ext.grid.Column({
					id : "colDateRec",
					text : Label.msg.list.date.rec,
					dataIndex : "dateRec",
					sortable : true,
					renderer : Ext.util.Format
							.dateRenderer(Label.format.date.liste),
					hidden : false,
					sortable : true,
					align: 'center',
					tdCls: 'wrapCell',
					autoSizeColumn: true,
					locked   : true,
					//maxWidth: 140
				}),
				new Ext.grid.Column({
					id : "colAppelant",
					text : Label.msg.list.num.appelant,
					dataIndex : "numeroAppelant",
					sortable : true,
					hidden : false,
					align: 'center',
					autoSizeColumn: true,
					locked   : true,
					//maxWidth: 200
				}),
				new Ext.grid.Column({
					id : "colDuree",
					text : Label.msg.list.duree,
					dataIndex : "duree",
					// sortable: true,
					hidden : false,
					renderer : renderDuree,
					align: 'center',
					autoSizeColumn: true,
					locked   : true,
					//maxWidth: 60
				}),
				new Ext.grid.Column({
					id : "colDisplayType",
					text : Label.msg.list.enrgt.typ,
					dataIndex : "displayType",
					sortable : true,
					hidden : false,
					renderer : renderDisplayType,
					align: 'center',
					autoSizeColumn: true,
					locked   : true,
					maxWidth: 45
				}),
			    new Ext.grid.Column({
					id : "colArchivage",
					text : Label.msg.list.archivage,
					dataIndex : "archivage",
					hidden : !canArchivage,
					isDisabled: !canArchivage,
					hideable : canArchivage,
					sortable : true,
					renderer: renderArchivage,
					autoSizeColumn: true,
					locked   : true,
					//maxWidth: 75
				})
			];
			// Loop keywords		 
			var j=0;
			//alert('Global.nombreMotCle:'+nbMotCleList+'');
			while(j<nbMotCleList){
				dynamicCols[dynamicCols.length] = new Ext.grid.Column({
					id : "colLibW"+String(j+1),
					text : Ext.libelleColonneCritereForResultGridList[j],
					dataIndex : "displayColW"+String(j+1),
					hidden : Ext.hiddeColCritereList[j],
					isDisabled: Ext.hiddeColCritereList[j],
					hideable : !Ext.hiddeColCritereList[j],
					sortable : true,
					autoSizeColumn: true,
					renderer: renderWithTooltip
				});
				j++;
			}
			dynamicCols[dynamicCols.length] = new Ext.grid.Column({
				id : "colNote",
				text : Label.msg.list.note,
				dataIndex : "note",
				sortable : false,
				visible : true,
				autoSizeColumn: true,
				renderer: renderWithTooltip
			});
			dynamicCols[dynamicCols.length] = new Ext.grid.Column({
				id : "colDataComp",
				text : libelleColonneDataCompForResultGrid,
				dataIndex : "displayDataCompCol",
				hidden : !isDataComp,
				isDisabled: !isDataComp,
				hideable : isDataComp,
				autoSizeColumn: true,
				sortable : false
			});
			
			dynamicCols[dynamicCols.length] = new Ext.grid.Column({
				id : "colToFitWidth",
				menuDisabled : true,
				hideable : false,
				sortable : false,
				minWidth: 0,
				flex: 1
			});

			// Create the grid used in result of the search.
			var grid = new Ext.grid.GridPanel(
					{
						stateful: true,
						stateId: 'messageGrid'+Ext.host,
						store : resultStore,
						width : Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
						forceFit : true,
						tbar : topAllPagingToolbar,
						bbar : bottomAllPagingToolbar,
						loadMask : true,
						selType : 'checkboxmodel',
						selModel : {
							checkOnly : true,
							injectCheckbox : 'first'
						},
						viewConfig : {
						    enableTextSelection: true,
							markDirty : false,
							listeners: {
								refresh: function(dataview) {
									if (!Ext.stateExists) {
						                Ext.each(dataview.panel.columns, function(column) {
						                    if (column.autoSizeColumn === true)
						                        column.autoSize();
						                });
									}
					            }
							},
							getRowClass: function(record, index, rowParams, store) {
						        if (record.get('nouveau')) {
						            return 'messageRow unReadMessageRow';
						        } else {
						            return 'messageRow';
						        }
						    }
						},
						listeners : {
							select : function(model, record, index, eOpts) {
								getNbSelected();
							},
							deselect : function(model, record, index, eOpts) {
								getNbSelected();
							}
						},
						columns : dynamicCols,
						style : {
							"margin-top" : "15px"
						}
					});

			var panel= new Ext.form.Panel({
				width : Ext.getBody().getViewSize().width*GRID_PAGING_WIDTH,
				items:[grid],
				renderTo : "grid"
			});

			// Hide refresh button
			topPagingToolbar.down('#refresh').hide();
			bottomPagingToolbar.down('#refresh').hide();
			
			// Add key listener of Pressing Enter.
			/*
			 * Ext.get("filtre").on("keypress", function(event) { if
			 * (event.getKey() == Ext.EventObject.ENTER) onSearchClicked(); });
			 */
			/**
			 * Handler of the button search. It reloads the result of the
			 * search.
			 */
			function onSearchClicked() {
				var param;

				// Get form fields and store them in the store baseParams
				// attribute for paging
				// to work.
				param = formMessageSearch.getForm().getValues(false);
				param['resetSelectedIds'] = true;
				// Reload page 1 and send search criteria.
				resultStore.loadPage(1, {
					params : param
				});
			}

			/**
			 * Handler of the clear criteria button. It clears out all the
			 * criteria
			 */
			function onClearClicked() {
				// Reset the form
				param = formMessageSearch.getForm().setValues({
					filtre : '0',
					cause : 'TOUS',
					flag_furtif : false,
					flag_del : false,
					dateDebut : null,
					heureDebut : null,
					dateFin : null,
					heureFin : null,
					refSvi : null,
					refEr : null,
					listRefEr : null,
					numAppelant : null,
					criteres : null,
					criteresData : null,
					pageSelectedIds: null,
					pageIds: null,
					resetForm: true
				});
				
				// affichage du nombre de selectionnes 
				nbIdsSelected=0;
				grid.getSelectionModel().deselectAll();
			    tabPageSelectedIds.length = 0;
				tabPageIds.length = 0;
				document.getElementById('pageSelectedIds').value = null;
				document.getElementById('pageIds').value = null;
				Ext.getCmp('listRefEr').reset();
				// Reload the list
				onSearchClicked();
			}

			function makePageIdTabs(store) {
				// alert("makePageIdTabs");
				var selectRecords = grid.getSelectionModel().getSelection();
				tabPageSelectedIds.length = 0;
				tabPageIds.length = 0;
				if (selectRecords != null) {
					for ( var i = 0; i < selectRecords.length; i++) {
						record = selectRecords[i];
						tabPageSelectedIds.push(record.get("msgId"));
					}
				}
				// alert("tabPageSelectedIds:length:"+tabPageSelectedIds.length+":value:"+tabPageSelectedIds+":");
				store.each(function(record, idx) {
					tabPageIds.push(record.get("msgId"));
				});
				// alert("tabPageIds:length:"+tabPageIds.length+":value:"+tabPageIds+":");
				return;
			}

			function setSelection(store) {
				var selectRecords = [];
				store.each(function(record, idx) {
					if (record.get("isSelected") != null
							&& (record.get("isSelected") == true)) {
						// alert('selected:'+record.get("msgId")+':==>PUSH_SELECTED:');
						selectRecords.push(record);
					}
				});
				// alert("tabPageIds:length:"+tabPageIds.length+":value:"+tabPageIds+":");
				grid.getSelectionModel().select(selectRecords);
				return;
			}
			
			function hasSelected() {
				var returnSelected = false;
				if(nbIdsSelected>0) returnSelected=true;
				return returnSelected;
			}
			
			function getNbSelected() {				
				var success = false;
				var resultText = null;
				makePageIdTabs(grid.getStore());
				var paramPageSelectedIds = null;
				var paramPageIds = null;
				if ((!(typeof tabPageSelectedIds === 'undefined'))
						&& tabPageSelectedIds != null
						&& tabPageSelectedIds.length > 0) {
					paramPageSelectedIds = tabPageSelectedIds.toString();
				}
				if ((!(typeof tabPageIds === 'undefined'))
						&& tabPageIds != null && tabPageIds.length > 0) {
					paramPageIds = tabPageIds.toString();
				}
				Ext.Ajax.request({
					async : false,
					url : "Json?req=NB_SELECTED_IDS_GET",
					method : "POST",
					params : {
						pageSelectedIds : paramPageSelectedIds,
						pageIds : paramPageIds
					},
					failure : function() {
						success = false;
					},
					success : function(response) {
						// Convert JSON response to a javascript object.
						try {
							resultText = Ext.decode(response.responseText),
							// alert("response:hasSelected:"+response.responseText+":");
							success = true;
						} catch (e) {
							// Could not parse response
							success = false;
						}
					}
				});
				if (success == true && resultText.rows[0].id >=0) {
					nbIdsSelected=resultText.rows[0].id;
					displayNbSelected();
				}	
				return ;
			}

			
			function displayNbSelected() {
				var nbSelectedLabel = Ext.getCmp('nbSelectedLabel');
				  if (document.getElementById) {
				    nbSelectedLabel.setText(nbIdsSelected  + ' ' + Label.message.nombre);
				  } else if (document.all) {
				    nbSelectedLabel.setText(nbIdsSelected  + ' ' + Label.message.nombre);
				  }
				  if (nbIdsSelected>0) {
					  Ext.getCmp('labelAction').setText(Label.grid.selection.action);
					  totalMessage = false;
				  } else { 
					  Ext.getCmp('labelAction').setText(Label.grid.selection.results);
					  totalMessage = true;
				  }
			}

			/**
			 * Called when a user displays a row.
			 * 
			 * @param {Ext.view.Table}
			 *            view The owning TableView.
			 * @param {Number}
			 *            rowIndex The row index clicked on.
			 * @param {Number}
			 *            colIndex The column index clicked on.
			 * @param {Object}
			 *            item The clicked item (or this Column if multiple
			 *            items were not configured).
			 * @param {Event}
			 *            e The click event.
			 * @param {Ext.data.Model}
			 *            record The Record underlying the clicked row.
			 */
			function viewItem(view, rowIndex, colIndex, item, e, record) {
				// Retrieve selected record identifier.
				var id = record.get("msgId");
				record.set('nouveau', false);
				
				// alert('colId:'+record.get("colId")+':MsgId:'+record.get("msgId")+':')
				var url = "EntryPoint?serviceName=EcouteMessageHandler&typeInfos=edit&idMsg="
						+ id;
				EasyRecord.popinItem("", url, grid);
				Ext.popinWindow.on('beforeclose', function () {
					if (childGetElementById && childGetElementById('messagePlayer') && (typeof childGetElementById('messagePlayer').pause() !== 'undefined')) {
						childGetElementById('messagePlayer').pause();
					}
				});
			}

			/**
			 * Execute mail method depending on the value of the field "locked".
			 * 
			 * @param {Ext.view.Table}
			 *            view The owning TableView.
			 * @param {Number}
			 *            rowIndex The row index clicked on.
			 * @param {Number}
			 *            colIndex The column index clicked on.
			 * @param {Object}
			 *            item The clicked item (or this Column if multiple
			 *            items were not configured).
			 * @param {Event}
			 *            e The click event.
			 * @param {Ext.data.Model}
			 *            record The Record underlying the clicked row.
			 */
			function mailItem(view, rowIndex, colIndex, item, e, record) {
				if(canMail){
					Ext.MessageBox.show({
						title : Label.help.info.title,
						msg : Label.confirm.envoi.message,
						buttons : Ext.MessageBox.YESNO,
						icon : Ext.MessageBox.WARNING,
						fn : function(buttonId) {
							mailItemConfirm(buttonId, view, rowIndex, colIndex,
									item, e, record);
						}
					});
				} else {
					EasyRecord.messageWarn(Label.nonAutorise);
				}
			}

			function mailItemConfirm(buttonId, view, rowIndex, colIndex, item,
					e, record) {
				if (buttonId != "yes")
					return;

				// Retrieve selected record identifier and forward to edit page.
				var id = record.get("msgId");
				document.actionForm.action = "EntryPoint?serviceName=MailMessageHandler&idMsg="
						+ id;
				document.actionForm.target = "iframeCachee";
				document.actionForm.submit();
			}

			function totalDel(){
				if(totalMessage && canDelete){
					EasyRecord.messageWarnYesNo(Label.msg.list.confirm.supprimer + countMessage + Label.msg.list.confirm.after, function(buttonId) {deleteRecords(buttonId);});
				}else {
					deleteItems();
				}
			}
			
			/**
			 * Delete all selected records in the grid
			 * 
			 */
			function deleteItems() {
				if(canDelete){
					EasyRecord.messageWarnYesNo(
							Label.msg.list.del.selected.messages.confirm,
							deleteRecords);
				} else {
					EasyRecord.messageWarn(Label.nonAutorise);
				}
			}
			/**
			 * Called when user clicks delete
			 * 
			 * @param {String}
			 *            buttonId Button identifier.
			 */
			function deleteRecords(buttonId) {
				// User did not click yes button, exit.
				if (buttonId != "yes")
					return;

				// var store = grid.getStore();
				var wholeSuccess = true;
				var param = formMessageSearch.getForm().getValues(false);
				grid.setLoading(true);
				makePageIdTabs(grid.getStore());
				if ((!(typeof tabPageSelectedIds === 'undefined'))
						&& tabPageSelectedIds != null
						&& tabPageSelectedIds.length > 0) {
					param['pageSelectedIds'] = tabPageSelectedIds.toString();
				}
				if ((!(typeof tabPageIds === 'undefined'))
						&& tabPageIds != null && tabPageIds.length > 0) {
					param['pageIds'] = tabPageIds.toString();
				}
				Ext.Ajax.request({
					async : false,
					url : "Json?req=MESSAGE_DELETE",
					method : "POST",
					params : param,
					failure : function() {
						wholeSuccess = false;
					},
					success : function(response) {
						// Convert JSON response to a javascript object.
						try {
							result = Ext.decode(response.responseText);
						} catch (e) {
							// Could not parse response, build a response
							// ourself.
							result = {
								success : false,
								message : Label.global.error.msg
							};
						}
						if (!result.success)
							wholeSuccess = false;
					}
				});
				grid.setLoading(false);
				if (wholeSuccess) {
					Ext.MessageBox.show({
						title : Label.help.info.title,
						msg : Label.msg.deleteok,
						cls : "msgButton", 
						buttons : Ext.MessageBox.OK,
						icon : Ext.MessageBox.INFO,
						// We need to reload the store to update it accordingly
						// to the deleted row
						fn : onSearchClicked()
					});
				} else {
					// Display error message.
					// alert(result.message);
					Ext.MessageBox.show({
						title : Label.global.error.msg,
						msg : result.message,
						cls : "msgButton", 
						buttons : Ext.MessageBox.OK,
						icon : Ext.MessageBox.ERROR
					});
				}
			}
		
			/**
			 * Download csv all selected records in the grid
			 * 
			 */
			function csvItems() {
				makePageIdTabs(grid.getStore());
				if ((!(typeof tabPageSelectedIds === 'undefined'))
						&& tabPageSelectedIds != null
						&& tabPageSelectedIds.length > 0) {
					// param['pageSelectedIds']=tabPageSelectedIds.toString();
					document.getElementById('pageSelectedIds').value = tabPageSelectedIds
							.toString();
				}
				if ((!(typeof tabPageIds === 'undefined'))
						&& tabPageIds != null && tabPageIds.length > 0) {
					// param['pageIds']=tabPageIds.toString();
					document.getElementById('pageIds').value = tabPageIds
							.toString();
				}

				// alert(document.getElementById('pageSelectedIds').value);
				document.actionForm.action = "EntryPoint?serviceName=ExportMessagesHandler&export_action=csv&mtime="
						+ new Date().getTime();
				// document.actionForm.onsubmit="monPop = window.open('',
				// 'csvLoad', '')";
				document.actionForm.submit();
			}

			
			function totalZip(){
				if(totalMessage){
					EasyRecord.messageWarnYesNo(Label.msg.list.confirm.telechargerZIP + countMessage + Label.msg.list.confirm.after, function(buttonId) {messageButtonToolBarZip(buttonId);});
				}else {
					zipItems();
				}
			}
			/**
			 * Called when user clicks any button of the modal window.
			 * 
			 * @param {String} buttonId Button identifier.
			 */
			function messageButtonToolBarZip(buttonId){
				if(buttonId != "yes"){
					return;
				}else {
					if(countMessage > globalmaxNbMsgExportZip){
						EasyRecord.messageWarn(Label.nonAutorise);
					}else {
						zipItems();
					}
				}
			}
			
			function totalCsv(){
				if(totalMessage){
					EasyRecord.messageWarnYesNo(Label.msg.list.confirm.telechargerCSV + countMessage + Label.msg.list.confirm.after, function(buttonId) {messageButtonToolBarCsv(buttonId);});
				}else {
					csvItems();
				}
			}
			/**
			 * Called when user clicks any button of the modal window.
			 * 
			 * @param {String} buttonId Button identifier.
			 */
			function messageButtonToolBarCsv(buttonId){
				if(buttonId != "yes"){
					return;
				}else {
					csvItems();
				}
			}
			
			/**
			 * Download zip all selected records in the grid
			 * 
			 */
			function zipItems() {
				if(canTelechargMass){
					if(totalMessage && (countMessage > globalmaxNbMsgExportZip)) {
						EasyRecord.messageWarn(Label.nonAutorise);
					}else {
						makePageIdTabs(grid.getStore());
						if ((!(typeof tabPageSelectedIds === 'undefined'))
								&& tabPageSelectedIds != null
								&& tabPageSelectedIds.length > 0) {
							document.getElementById('pageSelectedIds').value = tabPageSelectedIds
									.toString();
						}
						if ((!(typeof tabPageIds === 'undefined'))
								&& tabPageIds != null && tabPageIds.length > 0) {
							document.getElementById('pageIds').value = tabPageIds
									.toString();
						}
		
						// document.valeur.submit();
						// alert(document.getElementById('pageSelectedIds').value);
						document.actionForm.action = "EntryPoint?serviceName=ExportMessagesHandler&export_action=zip&mtime="
								+ new Date().getTime();
						// document.actionForm.onsubmit="monPop = window.open('',
						// 'zipLoad', '')";
						document.actionForm.submit();
					}
				} else {
					EasyRecord.messageWarn(Label.nonAutorise);
				}
			}

			/**
			 * Delete all selected records in the grid
			 * 
			 */
			function keepItem(view, rowIndex, colIndex, item, e, record) {
				if(canDelete && accessToMsgFlagDelete){
					Ext.MessageBox.show({
						title : Label.help.info.title,
						msg : Label.confirm.keep.message,
						cls : "msgButton", 
						buttons : Ext.MessageBox.YESNO,
						icon : Ext.MessageBox.WARNING,
						fn : function(buttonId) {
							keepItemConfirm(buttonId, record);
						}
					});
				} else {
					EasyRecord.messageWarn(Label.nonAutorise);
				}
			}
			/**
			 * Called when user clicks delete
			 * 
			 * @param {String}
			 *            buttonId Button identifier.
			 */
			function keepItemConfirm(buttonId, record) {
				// User did not click yes button, exit.
				if (buttonId != "yes")
					return;
				// var store = grid.getStore();
				var wholeSuccess = true;
				var param = formMessageSearch.getForm().getValues(false);
				grid.setLoading(true);
				param['idMsg'] = record.get("msgId");
				Ext.Ajax.request({
					async : false,
					url : "Json?req=MESSAGE_KEEP",
					method : "POST",
					params : param,
					failure : function() {
						wholeSuccess = false;
					},
					success : function(response) {
						// Convert JSON response to a javascript object.
						try {
							result = Ext.decode(response.responseText);
						} catch (e) {
							// Could not parse response, build a response
							// ourself.
							result = {
								success : false,
								message : Label.global.error.msg
							};
						}
						if (!result.success)
							wholeSuccess = false;
					}
				});
				grid.setLoading(false);
				if (wholeSuccess) {
					Ext.MessageBox.show({
						title : Label.help.info.title,
						msg : Label.msg.keepok,
						cls : "msgButton", 
						buttons : Ext.MessageBox.OK,
						icon : Ext.MessageBox.INFO,
						// We need to reload the store to update it accordingly
						// to the deleted row
						fn : onSearchClicked()
					});
				} else {
					// Display error message.
					// alert(result.message);
					Ext.MessageBox.show({
						title : Label.global.error.msg,
						msg : result.message,
						cls : "msgButton", 
						buttons : Ext.MessageBox.OK,
						icon : Ext.MessageBox.ERROR
					});
				}
			}

			/**
			 * Execute archive msg
			 * 
			 * @param {Ext.view.Table}
			 *            view The owning TableView.
			 * @param {Number}
			 *            rowIndex The row index clicked on.
			 * @param {Number}
			 *            colIndex The column index clicked on.
			 * @param {Object}
			 *            item The clicked item (or this Column if multiple
			 *            items were not configured).
			 * @param {Event}
			 *            e The click event.
			 * @param {Ext.data.Model}
			 *            record The Record underlying the clicked row.
			 */
			function archiveItem(view, rowIndex, colIndex, item, e, record) {
				if(canArchivage){
					Ext.MessageBox.show({
						title : Label.help.info.title,
						msg : Label.confirm.archive.message,
						cls : "msgButton", 
						buttons : Ext.MessageBox.YESNO,
						icon : Ext.MessageBox.WARNING,
						fn : function(buttonId) {
							archiveItemConfirm(buttonId, record);
						}
					});
				} else {
					EasyRecord.messageWarn(Label.nonAutorise);
				}
			}

			function archiveItemConfirm(buttonId, record) {
				// User did not click yes button, exit.
				if (buttonId != "yes")
					return;
				// var store = grid.getStore();
				var wholeSuccess = true;
				var param = formMessageSearch.getForm().getValues(false);
				grid.setLoading(true);
				param['idMsg'] = record.get("msgId");
				if (record.get("archivage") == 0)
					param['statutArchivage'] = 1;
				else if (record.get("archivage") == 1)
					param['statutArchivage'] = 0;
				Ext.Ajax.request({
					async : false,
					url : "Json?req=MESSAGE_ARCHIVE",
					method : "POST",
					params : param,
					failure : function() {
						wholeSuccess = false;
					},
					success : function(response) {
						// Convert JSON response to a javascript object.
						try {
							result = Ext.decode(response.responseText);
						} catch (e) {
							// Could not parse response, build a response
							// ourself.
							result = {
								success : false,
								message : Label.global.error.msg
							};
						}
						if (!result.success)
							wholeSuccess = false;
					}
				});
				grid.setLoading(false);
				if (wholeSuccess) {
					Ext.MessageBox.show({
						title : Label.help.info.title,
						msg : Label.msg.archiveok,
						cls : "msgButton", 
						buttons : Ext.MessageBox.OK,
						icon : Ext.MessageBox.INFO,
						// We need to reload the store to update it accordingly
						// to the deleted row
						fn : onSearchClicked()
					});
				} else {
					// Display error message.
					// alert(result.message);
					Ext.MessageBox.show({
						title : Label.global.error.msg,
						msg : result.message,
						cls : "msgButton", 
						buttons : Ext.MessageBox.OK,
						icon : Ext.MessageBox.ERROR
					});
				}
			}
			
	/**
	 * Display legend
	 * 
	 */
	function displayLegend() {
		//EasyRecord.popinItem(Label.msg.legende.title, urlLegende, grid);
		//window.open(urlLegende);
		//EasyRecord.popinItem(Label.msg.legende.title, "index_old.html", grid);
		if (dlgPopup == null){
			var store = Ext.create('Ext.data.Store', {
				pageSize: 10, // For paging
				remoteSort: false,
				storeId:'bidonLegendStore',
				fields:['typeE','actionId'],
				data:{'items':[
				    // 1 enregistrement avec valeur qu on veut c est pour appeler render
					{ 'typeE':'typeQuOnVeut', 'actionId': 'actionQuOnVeut' }
				]},
				proxy: {
					type: 'memory',
					reader: {
						type: 'json',
						root: 'items'
					}
				}
			});
			var TabLegend=Ext.create('Ext.grid.Panel', {
				store: store,
				columns: {
					defaults: { resizable:false, sortable : false, hideable : false, draggable : false, menuDisabled : true },
					items: [		
						{ text: Label.msg.legende.type,  dataIndex: 'typeE', flex:50, renderer: renderImgTypeE},
						{ text: Label.msg.legende.action,  dataIndex: 'actionId', flex:50, renderer: renderImgActions}
					]
				}
			});
			dlgPopup = new Ext.Window({
				//el:'win_req_in',
				modal:true,
				layout:'fit',
				width:500,
				height:250,
				closable:false,
				resizable:false,
				plain:true,
				items:[TabLegend],
				buttons:[{
					text: Label.fermer,
					handler:function() {
						dlgPopup.hide();
					}
				}]
			});
			dlgPopup.show();
		} else {
			dlgPopup.show();
		}
	}

	function renderImgTypeE (value, metaData, record, rowIndex, colIndex, store, view) {			
		var html='<table>'; 
		html+='<tr class="txt10"><td width="16"><img src="images/new_icons/filtre16.png" border=0></td><td>'+Label.msg.legende.filtre+'</td></tr>';
		html+='<tr class="txt10"><td><img src="images/new_icons/agent16.png" border=0></td><td>'+Label.msg.legende.agent+'</td></tr>';
		html+='<tr class="txt10"><td><img src="images/new_icons/systematique16.png" border=0></td><td>'+Label.msg.legende.all+'</td></tr>';
		if (globalFurtifOption!=Constantes.FURTIF_DISABLED && (accesToMsgFurtif || globalFurtifOption==Constantes.FURTIF_FORCED))
			html+='<tr class="txt10"><td><img src="images/new_icons/furtif16.png" border=0></td><td>'+Label.search.msg.msg.furtif.rec+'</td></tr>';
		html+='</table>';			
		return html;
	}
			
	function renderImgActions (value, metaData, record, rowIndex, colIndex, store, view) {			
		var html='<table>';
		html+='<tr class="txt10"><td width="16"><img src="images/new_icons/hp16.png" border=0></td><td>'+Label.ecouter+'</td></tr>';
		if (canMail)
			html+='<tr class="txt10"><td><img src="images/new_icons/mail16.png" border=0></td><td>'+Label.emailer+'</td></td></tr>';
		if (canArchivage)
			html+='<tr class="txt10"><td><img src="images/new_icons/tape16.png" border=0></td><td>'+Label.searchMsgArchive+'</td></tr>';
		if (canDelete && accessToMsgFlagDelete)
			html+='<tr class="txt10"><td><img src="images/new_icons/deleteFl16.gif" border=0></td><td>'+Label.conserver+'</td></tr>';
		html+='</table>';			
		return html;
	}
		
	function renderArchivage(value, metaData, record, rowIndex,
		colIndex, store, view) {
		var html = '{0}';
		var chaine= '&nbsp;';
		if (value == 0) {
			chaine=Label.statutArchivage_0;
			//chaine='archivage_0';
		} else if (value == 1) {
			chaine=Label.statutArchivage_1;
			//chaine='archivage_1';
		} else if (value == 2) {
			chaine=Label.statutArchivage_2;
		} else if (value == 3) {
			chaine=Label.statutArchivage_3;
		} else if (value == 99) {
			chaine=Label.statutArchivage_99;
		}
		return Ext.String.format(html,chaine);
	}

	function renderDisplayType(value, metaData, record, rowIndex,
			colIndex, store, view) {
		var html = '<img src="{0}" alt="{1}" data-qtip="{1}"/>';
		if (value == 0) {
			return Ext.String.format(html,
					'images/new_icons/furtif16.png',
					Label.search.msg.msg.furtif.rec,
					Label.search.msg.msg.furtif.rec);
		} else if (value == 1) {
			return Ext.String.format(html,
					'images/new_icons/agent16.png',
					Label.search.msg.msg.agt.rec,
					Label.search.msg.msg.agt.rec);
		} else if (value == 2) {
			return Ext.String.format(html,
					'images/new_icons/systematique16.png',
					Label.search.msg.msg.all.rec,
					Label.search.msg.msg.all.rec);
		} else {
			return Ext.String.format(html,
					'images/new_icons/filtre16.png',
					Label.search.msg.msg.filtre.rec,
					Label.search.msg.msg.filtre.rec);
		}
	}

	function renderDuree(value, metaData, record, rowIndex, colIndex,
					store, view) {
		var html = '{0}';
		var chaine = "";
		var min, sec;
		if (value < 10) {
			// exemple 00'02''
			chaine = "00'0" + value + "''";
		} else if (value < 60) {
			// exemple 00'32''
			chaine = "00'" + value + "''";
		} else {
			min = Math.floor(value / 60);
			sec = value - (min * 60);
			if (min < 10) {
				if (sec < 10)
					// exemple 08'02''
					chaine = "0" + min + "'0" + sec + "''";
				else
					// exemple 08'32''
					chaine = "0" + min + "'" + sec + "''";
			} else {
				if (sec < 10)
					// exemple 18'02''
					chaine = min + "'0" + sec + "''";
				else
					// exemple 18'32''
					chaine = min + "'" + sec + "''";
			}
		}
		return Ext.String.format(html, chaine);
	}
	
	function renderWithTooltip(value, metaData, record, rowIndex, colIndex, store, view) {
		var html = '<span data-qtip="{0}">{1}</span>';
		return Ext.String.format(html,value, value);
	}
	
	// Message result count in grid and setting result label
	function getGridResultCount() {
		resultStore.on('load', function(ds){ 
			countMessage = resultStore.getTotalCount();
			if(resultStore.getTotalCount() > 0){
				Ext.getCmp('deleteButton').setDisabled(false);
				Ext.getCmp('zipButton').setDisabled(false);
				Ext.getCmp('csvButton').setDisabled(false);
			}else {
				Ext.getCmp('deleteButton').setDisabled(true);
				Ext.getCmp('zipButton').setDisabled(true);
				Ext.getCmp('csvButton').setDisabled(true);
			}
            var resultLabel = Ext.getCmp('resultLabelId');
            resultLabel.setText(Label.result.toUpperCase()  + ' : ' + resultStore.getTotalCount());
            if(resultStore.getTotalCount()<1)
            	Ext.getCmp('fullWindowButton').setVisible(false);
		});  
	}
	
	function onFullWindowClicked() {
		var me=grid;
		var height = me.getHeight();
		var width = me.getWidth();
		var parentPanel = me.findParentByType('panel');
		parentPanel.remove(me, false);
		Ext.getBody().setStyle('overflow', 'hidden');
		var maxHeight = me.maxHeight;
		me.setWidth(Ext.getBody().getWidth() - 25);
		
		var me2 = buttonToolbar;
		var width2 = me2.getWidth();
		me2.setWidth(Ext.getBody().getWidth() - 25);
		
		
		var fullPopinGridWindow = new Ext.Window({
			renderTo: Ext.getBody(),
			maximized: true,
			autoDestroy: true,
			overflowY: 'scroll',
			layout: {
				type: 'vbox',
				align: 'center'
			},
			modal: true,
			style: {
				top: '0px'
			},
			closeAction: 'destroy',
			items: [{
				xtype: 'panel',
				ui: 'popupform',
				items: [me2, me]
			}],
			listeners: {
				'close': function(thisWindow, opts) {
					Ext.getBody().setStyle('overflow', 'auto');
					thisWindow.remove(me, false);
					thisWindow.remove(me2, false);
					me2.setWidth(width2);
					me.setWidth(width);
					parentPanel.add(me2);
					parentPanel.add(me);
				},
				'show': function(thisWidow) {
					if (!Ext.isIE) {
						thisWidow.setPosition(Ext.getBody().getScrollLeft(),
								Ext.getBody().getScrollTop());
					}
				}
			}
		}).show();

	}
	
	getGridResultCount();
	
	// Set focus on first field.
	formMessageSearch.getComponent("filtre").focus();
	
	// Fire search now to populate list
	if (Ext.auto_load_message != 0)
		onSearchClicked();
	
	//Allow public access to refresh function for popin window
	Ext.onSearchClicked = onSearchClicked;

	//Set title line width to match the extended message view
	Ext.select('.title').setStyle('width', Ext.getCmp('formMessageSearch').getWidth()+'px');
});
