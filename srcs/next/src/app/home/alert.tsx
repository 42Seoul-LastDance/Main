import React, { useState, useEffect, Dispatch } from 'react';
import Alert, { AlertColor } from '@mui/material/Alert'; // import AlertColor
import { setAlertMsg, setSeverity, setShowAlert } from '../redux/alertSlice';
import { useDispatch, useSelector } from 'react-redux';
import { RootState } from '../redux/store';
import { Typography } from '@mui/material';

interface HeaderAlertProps {
  severity: AlertColor; // restrict severity to AlertColor
}

export const myAlert = (
  severity: string,
  msg: string,
  dispatch: Dispatch<any>,
) => {
  dispatch(setSeverity(severity));
  dispatch(setAlertMsg(msg));
  dispatch(setShowAlert(true));
};

const HeaderAlert: React.FC<HeaderAlertProps> = ({}) => {
  const showAlert = useSelector((state: RootState) => state.alert.showAlert);
  const message = useSelector((state: RootState) => state.alert.alertMsg);
  const severity = useSelector(
    (state: RootState) => state.alert.severity,
  ) as AlertColor; // cast severity as AlertColor
  const timeout = 3000;
  const dispatch = useDispatch();

  useEffect(() => {
    const timer = setTimeout(() => {
      dispatch(setShowAlert(false));
    }, timeout);
  }, [showAlert, timeout]);

  return showAlert ? (
    <div className="alert-container">
      <Alert
        sx={{
          borderRadius: '15px',
          zIndex: 2,
        }}
        severity={severity}
      >
        <Typography variant="h6">{message}</Typography>
      </Alert>
    </div>
  ) : null;
};

export default HeaderAlert;
