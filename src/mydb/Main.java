/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mydb;

import javax.swing.JOptionPane;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

/**
 *
 * @author JDE
 */
public class Main<T> extends javax.swing.JFrame {

    /**
     * Creates new form Main
     */
    
    private Database db;
    private Schema schema;
    private String db_folder_url;
    // private Table table;
    private boolean isInitializing; 
    private int editableRow;
    private int editableCol;
    private String origVal;
    private String newVal;
    private boolean isAdding;   
    private String tableName;
    
    public Main() {
        initComponents();
        
        this.isInitializing = true;
        
        // initialize database object
        this.db_folder_url = "c:/MyDB";
        
         
        // initialize database object
        this.db = new Database(this.db_folder_url + "/db1", this.db_folder_url + "/master.schema");
        
        if(this.db == null){
            System.out.println("Error connecting to database folder");
        }
        
        
        // populate database list
        this.cmbDB.addItem("db1");
        
        // populate tables list
        this.cmbTables.addItem("student");
        this.cmbTables.addItem("table1");
        this.cmbTables.addItem("table2");
        
        // make sure nothing is selected yet
        this.cmbTables.setSelectedIndex(-1);
        
        // hide table for now
        this.tblTable.setVisible(false);
        this.isInitializing = false;
       
        
        this.editableRow = -1;   // make everything editable
        this.editableCol = -1;
        
        this.origVal = null;
        this.newVal = null;
        
        this.isAdding = false;
        
        this.tableName = "";
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jLabel1 = new javax.swing.JLabel();
        cmbDB = new javax.swing.JComboBox<>();
        jLabel2 = new javax.swing.JLabel();
        cmbTables = new javax.swing.JComboBox<>();
        jScrollPane1 = new javax.swing.JScrollPane();
        tblTable = new javax.swing.JTable();
        lblRecCount = new javax.swing.JLabel();
        cmdAdd = new javax.swing.JButton();
        cmdUpdate = new javax.swing.JButton();
        cmdCancel = new javax.swing.JButton();
        cmdDel = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        jLabel1.setText("Choose Database:");

        cmbDB.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmbDBActionPerformed(evt);
            }
        });

        jLabel2.setText("Choose Table:");

        cmbTables.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmbTablesActionPerformed(evt);
            }
        });
        cmbTables.addPropertyChangeListener(new java.beans.PropertyChangeListener() {
            public void propertyChange(java.beans.PropertyChangeEvent evt) {
                cmbTablesPropertyChange(evt);
            }
        });

        tblTable.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        tblTable.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                tblTableFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                tblTableFocusLost(evt);
            }
        });
        tblTable.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tblTableMouseClicked(evt);
            }
        });
        tblTable.addPropertyChangeListener(new java.beans.PropertyChangeListener() {
            public void propertyChange(java.beans.PropertyChangeEvent evt) {
                tblTablePropertyChange(evt);
            }
        });
        jScrollPane1.setViewportView(tblTable);

        lblRecCount.setText("0 Records");

        cmdAdd.setText("Add Record");
        cmdAdd.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmdAddActionPerformed(evt);
            }
        });

        cmdUpdate.setText("Update");
        cmdUpdate.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmdUpdateActionPerformed(evt);
            }
        });

        cmdCancel.setText("Cancel");
        cmdCancel.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmdCancelActionPerformed(evt);
            }
        });

        cmdDel.setText("Del Record");
        cmdDel.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmdDelActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(27, 27, 27)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(cmdAdd)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cmdDel)
                        .addGap(128, 128, 128)
                        .addComponent(cmdCancel)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cmdUpdate))
                    .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addComponent(lblRecCount)
                        .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 452, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGroup(layout.createSequentialGroup()
                            .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                .addComponent(jLabel1)
                                .addComponent(jLabel2))
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                .addComponent(cmbTables, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addComponent(cmbDB, javax.swing.GroupLayout.PREFERRED_SIZE, 166, javax.swing.GroupLayout.PREFERRED_SIZE)))))
                .addContainerGap(25, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(23, 23, 23)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel1)
                    .addComponent(cmbDB, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel2)
                    .addComponent(cmbTables, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(3, 3, 3)
                .addComponent(lblRecCount)
                .addGap(1, 1, 1)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 202, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(cmdAdd)
                    .addComponent(cmdUpdate)
                    .addComponent(cmdCancel)
                    .addComponent(cmdDel))
                .addContainerGap(46, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void cmbDBActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmbDBActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_cmbDBActionPerformed

    private void cmbTablesActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmbTablesActionPerformed
        if(this.isInitializing ){
            return;
        }
        
        // System.out.println(this.cmbTables.getSelectedItem());
        if( this.cmbTables.getSelectedIndex() == -1 ){
            return;
        }
        
        // get selected table name
        String table_name = this.cmbTables.getSelectedItem().toString();
        System.out.println(table_name);
        
        // save table name
        this.tableName = table_name;
        
        Table table = db.getTable(table_name);
        
        // get the list of fields for this table
        String[] flds = table.getFieldNames();
        
        // show table 
        this.tblTable.setVisible(true);
        
        // create a blank table model
        DefaultTableModel dtm = new DefaultTableModel();
        
        // dynamically add the columns based on the field
        for(int i=0; i<flds.length; i++){
            dtm.addColumn(flds[i]);
        }
        
        // attemp to get all the records
        Field[] fields = new Field[flds.length];
        for(int i=0; i<flds.length; i++){
            fields[i] = table.getField(i);
        }
        Recordset rs = db.getRecords(table, fields, null);
        
        // make sure the table is not empty before adding
        if( rs.getRecordCount() > 0  ){ // this could also be if( !rs.isEmpty() )
            // dynamically add rows to the table
            while(rs.hasNext()){
                Record rec = rs.nextRecord();

                String[] vals = new String[flds.length];

                for(int i=0; i<vals.length; i++){
                    vals[i] = rec.getFieldValue(flds[i]).toString();
                }

                dtm.addRow(vals);
            }
        }
        
        // display total records
        this.lblRecCount.setText(rs.getRecordCount() + " Record(s)");
        
        // apply table model
        this.tblTable.setModel(dtm);
        
        // assign a CellEditorlistener
        CellEditorListener cel = new CellEditorListener() {
            @Override
            public void editingStopped(ChangeEvent ce) {
                if( isAdding == true){
                    return;
                }
                
                if(editableCol == -1 || editableRow == -1){ // don't do anything when row and col index are invalid
                    return;
                }
                
                // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                DefaultTableModel dtm = (DefaultTableModel) tblTable.getModel();
                
                // save new value to property
                newVal = (String) dtm.getValueAt(editableRow, editableCol);
                
                // showInfoMsg(String.valueOf(newVal));
                
                // if something changed, save it to database
                if( newVal != origVal ){
                    // get column/field name
                    String fieldname = dtm.getColumnName( editableCol );
                    Field field = table.getField(fieldname);
                    
                    // create data values 
                    DataValue[] dv = new DataValue[1];
                    dv[0] = new DataValue(fieldname, db.toCorrectData(newVal,field ));
                    
                    // create filter expression
                    FilterExpression[] fexpr = new FilterExpression[dtm.getColumnCount()];
                    for( int i=0; i<fexpr.length; i++ ){
                        // get current value col
                        String val = (String) dtm.getValueAt(editableRow, i);
                        Field f = table.getField(dtm.getColumnName(i));
                        
                        // check if current field is the one changed so you can use old value
                        if( i == editableCol  ){
                            val = origVal;
                        }
                        
                        if( i == 0 ){
                            
                            fexpr[i] = new FilterExpression(new FilterTerm( table.getField(dtm.getColumnName(i))), DBRelationalOptr.EQUAL, new FilterTerm(db.toCorrectData(val, f)), null, null);
                        }
                        else {
                            fexpr[i] = new FilterExpression(new FilterTerm(table.getField(dtm.getColumnName(i))), DBRelationalOptr.EQUAL, new FilterTerm(db.toCorrectData(val, f)), DBLogicalOptr.AND, fexpr[i-1]);
                        }
                    }
                    
                    
                    // attempt to update table
                    if( db.updateTable(table.getName(), dv, fexpr[fexpr.length-1]) == 0  ){
                        showInfoMsg("Table updated successfully");
                    }
                    else {
                        showInfoMsg("Update failed!");
                    }
                    
                    // reset edit-related properties
                    origVal = null;
                    newVal = null;
                    editableCol = -1;
                    editableRow = -1;
                    
                    
                    
                }
            }

            @Override
            public void editingCanceled(ChangeEvent ce) {
                if( isAdding == true){
                    return;
                }
                
                // restore all value
                DefaultTableModel dtm = (DefaultTableModel) tblTable.getModel();
                dtm.setValueAt(origVal, editableRow, editableCol);
                
                // empty new value property
                newVal = null;
            }
        };
        
        this.tblTable.getDefaultEditor(String.class).addCellEditorListener(cel);
        
    }//GEN-LAST:event_cmbTablesActionPerformed

    private void cmbTablesPropertyChange(java.beans.PropertyChangeEvent evt) {//GEN-FIRST:event_cmbTablesPropertyChange
        // TODO add your handling code here:
         
    }//GEN-LAST:event_cmbTablesPropertyChange

    private void tblTableMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tblTableMouseClicked
        // TODO add your handling code here:
        if( this.editableRow > -1 && this.tblTable.getSelectedRow() != this.editableRow && this.isAdding == true ){
            this.tblTable.setRowSelectionInterval(this.editableRow, this.editableRow);
        }
        
        if( this.tblTable.getSelectedColumn() != -1 && this.tblTable.getSelectedRow() != -1 ){
            this.editableCol = this.tblTable.getSelectedColumn();
            this.editableRow = this.tblTable.getSelectedRow();
            
            // get value at current location
            DefaultTableModel dtm = (DefaultTableModel) this.tblTable.getModel();
            
            this.origVal = (String) dtm.getValueAt(this.editableRow, this.editableCol);
            
        }
        
        
        
    }//GEN-LAST:event_tblTableMouseClicked

    private void cmdAddActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmdAddActionPerformed
        // showInfoMsg("Add Record");
        
        // insert a blank row
        DefaultTableModel dtm = (DefaultTableModel) this.tblTable.getModel();
        dtm.addRow(new Object[]{});
        
        // auto-select/hi-light this row
        this.tblTable.setRowSelectionInterval(this.tblTable.getRowCount()-1, this.tblTable.getRowCount()-1);

        // lock other rows
        this.editableRow = this.tblTable.getSelectedRow();
        
        // indicate that you're adding
        this.isAdding = true;
        
        // disable Add Record button
        cmdAdd.setEnabled(false);
        
        // eable Cancel and Update buttons
        cmdCancel.setEnabled(true);
        cmdUpdate.setEnabled(true);
        
    }//GEN-LAST:event_cmdAddActionPerformed

    private void cmdUpdateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmdUpdateActionPerformed
        // attempt to insert record to database
        DefaultTableModel dtm = (DefaultTableModel)this.tblTable.getModel();
        
        DataValue[] dv = new DataValue[tblTable.getColumnCount()];
        
        Table table = db.getTable(this.tableName);
        
        int row = this.editableRow;
        for(int c=0; c<tblTable.getColumnCount(); c++){
            String column_name = tblTable.getColumnName(c);
            Field field = table.getField(column_name);
            System.out.println("Field: " + field.getName() + " T: " + field.getDatatype());
            Object val = db.toCorrectData(dtm.getValueAt(row, c).toString(), field);
            System.out.println("Object: " + val + " col: " + c);
            dv[c] = new DataValue( column_name, val);
        }
        
        db.insertToTable("student", dv);
        
        isAdding = false;
        
        // disable update and cancel button
        this.cmdUpdate.setEnabled(false);
        this.cmdCancel.setEnabled(false);
    }//GEN-LAST:event_cmdUpdateActionPerformed

    private void tblTablePropertyChange(java.beans.PropertyChangeEvent evt) {//GEN-FIRST:event_tblTablePropertyChange
        
        this.showInfoMsg("origVal is " + this.origVal + " newVal is " + newVal);
        
        if( origVal != null ){
            
            DefaultTableModel dtm = (DefaultTableModel) this.tblTable.getModel();
            
            newVal = (String) dtm.getValueAt(this.editableRow, this.editableCol);
            
        }
        
        
        
        
    }//GEN-LAST:event_tblTablePropertyChange

    private void tblTableFocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_tblTableFocusLost
        // TODO add your handling code here:
        
    }//GEN-LAST:event_tblTableFocusLost

    private void tblTableFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_tblTableFocusGained
        // TODO add your handling code here:
    }//GEN-LAST:event_tblTableFocusGained

    private void cmdCancelActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmdCancelActionPerformed
        if( isAdding == true ){
            // remove bottom-most row
            DefaultTableModel dtm = (DefaultTableModel) tblTable.getModel();
            dtm.removeRow(dtm.getRowCount()-1);
            
            editableRow = -1;
            editableCol = -1;
            
            isAdding = false;
            
            // hide Cancel and Update buttons
            cmdUpdate.setEnabled(false);
            cmdCancel.setEnabled(false);
            
            // enable Add Record button
            cmdAdd.setEnabled(true);
        }
    }//GEN-LAST:event_cmdCancelActionPerformed

    private void cmdDelActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmdDelActionPerformed
        // TODO add your handling code here:
        // check if a row is selected
        if( editableRow != -1  ){
            if(JOptionPane.showConfirmDialog(null, "Delete Record?", "App", JOptionPane.OK_CANCEL_OPTION) == JOptionPane.OK_OPTION ){
                DefaultTableModel dtm = (DefaultTableModel) tblTable.getModel();
                
                // get table
                Table table = db.getTable(this.tableName);
                
                // create filter expression
                FilterExpression[] fexpr = new FilterExpression[dtm.getColumnCount()];
                for( int i=0; i<fexpr.length; i++ ){
                    // get current value col
                    String val = (String) dtm.getValueAt(editableRow, i);
                    Field f = table.getField(dtm.getColumnName(i));

                    // check if current field is the one changed so you can use old value
                    if( i == 0 ){

                        fexpr[i] = new FilterExpression(new FilterTerm( table.getField(dtm.getColumnName(i))), DBRelationalOptr.EQUAL, new FilterTerm(db.toCorrectData(val, f)), null, null);
                    }
                    else {
                        
                        fexpr[i] = new FilterExpression(new FilterTerm(table.getField(dtm.getColumnName(i))), DBRelationalOptr.EQUAL, new FilterTerm(db.toCorrectData(val, f)), DBLogicalOptr.AND, fexpr[i-1]);
                    }
                }

                
                int res = db.deleteRecords(table, fexpr[fexpr.length-1]);
                
                if( res == 0 ){
                    // delete record from table
                    dtm.removeRow(editableRow);
                    
                    // show success msg
                    showInfoMsg("Record was successfully deleted!");
                    
                    editableCol = -1;
                    editableRow = -1;
                }
                else {
                    showInfoMsg("An error occured while deleting record!");
                    
                    
                }
                
            }
           
        }
        
        
    }//GEN-LAST:event_cmdDelActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(Main.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(Main.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Main.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Main.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new Main().setVisible(true);
                
               
            }
        });
    }
    
    public void showInfoMsg(String msg){
        JOptionPane.showMessageDialog(null, msg, "App", JOptionPane.INFORMATION_MESSAGE);
        
    }
    
    
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JComboBox<String> cmbDB;
    private javax.swing.JComboBox<String> cmbTables;
    private javax.swing.JButton cmdAdd;
    private javax.swing.JButton cmdCancel;
    private javax.swing.JButton cmdDel;
    private javax.swing.JButton cmdUpdate;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JLabel lblRecCount;
    private javax.swing.JTable tblTable;
    // End of variables declaration//GEN-END:variables
}
